"""What a run's EC2 instances cost: which instances its jobs ran on, their types and how long
they ran (from the run's start, or theirs, until they stopped or the run ended), priced at
AWS's on-demand list price or the Spot price history of their zone, plus their disks (EBS
volumes, noted while the instance exists, since they go with it). Each instance's cost is
shared out to the rules by their jobs' vCPU-time on it, idle time included.

Best effort: whatever can't be found out (no permission, an instance EC2 no longer knows) is
left to the rough per-job estimate in costs.py."""

import datetime
import json
import re
from collections import defaultdict
from typing import Optional

_STOPPED_AT = re.compile(r"\((\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) GMT\)")


def _utc(dt):
    return dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)


def _why(e, action):
    """A short reason: the missing permission for an access error, else the error's type."""
    text = str(e)
    if "AccessDenied" in text or "UnauthorizedOperation" in text or "not authorized" in text:
        return f"no permission for {action}"
    return type(e).__name__


def _ms(ts):
    return datetime.datetime.fromtimestamp(ts / 1000, datetime.timezone.utc)


class Instances:
    def __init__(self, ecs, ec2, region, started=None):
        self.ecs, self.ec2, self.region = ecs, ec2, region
        self.started = started or datetime.datetime.now(datetime.timezone.utc)
        self.ids = {}  # container instance ARN -> EC2 instance id (None: couldn't find out)
        self.volumes = {}  # EC2 instance id -> [(GiB, volume type)], or None if unknown
        self.vcpu_seconds = defaultdict(lambda: defaultdict(float))  # instance -> rule -> vCPU-s
        self.problem = None

    def note(self, arn):
        """Find the EC2 instance behind an ECS container instance, while it's still there."""
        if not arn or arn in self.ids:
            return
        self.ids[arn] = None
        cluster = arn.split(":container-instance/", 1)[-1].split("/")[0] if "/" in arn else None
        try:
            kw = {"containerInstances": [arn]}
            if cluster and cluster != arn:
                kw["cluster"] = cluster
            found = self.ecs.describe_container_instances(**kw).get("containerInstances") or []
            if found:
                self.ids[arn] = found[0].get("ec2InstanceId")
        except Exception as e:
            self.problem = self.problem or _why(e, "ecs:DescribeContainerInstances")
            return
        iid = self.ids.get(arn)
        if iid and iid not in self.volumes:
            self.volumes[iid] = None
            try:
                r = self.ec2.describe_instances(InstanceIds=[iid])["Reservations"]
                vol_ids = [m["Ebs"]["VolumeId"] for res in r for inst in res["Instances"]
                           for m in inst.get("BlockDeviceMappings", []) if m.get("Ebs", {}).get("VolumeId")]
                if vol_ids:
                    vols = self.ec2.describe_volumes(VolumeIds=vol_ids)["Volumes"]
                    self.volumes[iid] = [(v["Size"], v.get("VolumeType", "gp3")) for v in vols]
            except Exception:
                pass  # priced without its disk

    def add_job(self, rule, d, vcpu):
        """Count a finished job's vCPU-time on the instances it ran on (every attempt). False
        if any attempt's instance is unknown."""
        attempts = d.get("attempts") or [dict(container=d.get("container") or {},
                                              startedAt=d.get("startedAt"), stoppedAt=d.get("stoppedAt"))]
        known = True
        for att in attempts:
            start, stop = att.get("startedAt"), att.get("stoppedAt")
            if start is None or stop is None:
                continue
            arn = (att.get("container") or {}).get("containerInstanceArn")
            self.note(arn)
            iid = self.ids.get(arn)
            if not iid:
                known = False
                continue
            self.vcpu_seconds[iid][rule] += vcpu * max(0, stop - start) / 1000
        return known

    # -- prices --------------------------------------------------------------------------
    def _on_demand(self, pricing, itype):
        """(USD per hour, vCPUs) from AWS's price list."""
        filters = [{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in (
            ("regionCode", self.region), ("instanceType", itype), ("operatingSystem", "Linux"),
            ("tenancy", "Shared"), ("preInstalledSw", "NA"), ("capacitystatus", "Used"))]
        for item in pricing.get_products(ServiceCode="AmazonEC2", Filters=filters, MaxResults=10)["PriceList"]:
            p = json.loads(item) if isinstance(item, str) else item
            vcpu = int(p.get("product", {}).get("attributes", {}).get("vcpu", 0) or 0)
            for term in p.get("terms", {}).get("OnDemand", {}).values():
                for dim in term.get("priceDimensions", {}).values():
                    usd = float(dim.get("pricePerUnit", {}).get("USD", 0) or 0)
                    if usd > 0:
                        return usd, vcpu
        raise LookupError(f"no on-demand price for {itype} in {self.region}")

    def _volume_price(self, pricing, vtype):
        """USD per GB-month for an EBS volume type, from AWS's price list."""
        filters = [{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in (
            ("regionCode", self.region), ("productFamily", "Storage"), ("volumeApiName", vtype))]
        for item in pricing.get_products(ServiceCode="AmazonEC2", Filters=filters, MaxResults=10)["PriceList"]:
            p = json.loads(item) if isinstance(item, str) else item
            for term in p.get("terms", {}).get("OnDemand", {}).values():
                for dim in term.get("priceDimensions", {}).values():
                    usd = float(dim.get("pricePerUnit", {}).get("USD", 0) or 0)
                    if usd > 0:
                        return usd
        raise LookupError(f"no price for {vtype} volumes in {self.region}")

    def _spot(self, itype, az, start, end):
        """Average Spot price (USD per hour) of the type in the zone over the instance's time."""
        hist = self.ec2.describe_spot_price_history(
            InstanceTypes=[itype], ProductDescriptions=["Linux/UNIX"], AvailabilityZone=az,
            StartTime=start, EndTime=end)["SpotPriceHistory"]
        if not hist:
            hist = self.ec2.describe_spot_price_history(
                InstanceTypes=[itype], ProductDescriptions=["Linux/UNIX"], AvailabilityZone=az,
                MaxResults=1)["SpotPriceHistory"]
        prices = [float(h["SpotPrice"]) for h in hist]
        if not prices:
            raise LookupError(f"no Spot price for {itype} in {az}")
        return sum(prices) / len(prices)

    def costs(self, pricing, now=None):
        """({rule: USD}, summary line) for the instances, or (None, why not)."""
        if not self.vcpu_seconds:
            return None, self.problem or "no EC2 instances seen"
        now = now or datetime.datetime.now(datetime.timezone.utc)
        try:
            found = {}
            ids = list(self.vcpu_seconds)
            for i in range(0, len(ids), 100):
                for r in self.ec2.describe_instances(InstanceIds=ids[i:i + 100])["Reservations"]:
                    for inst in r["Instances"]:
                        found[inst["InstanceId"]] = inst
        except Exception as e:
            return None, _why(e, "ec2:DescribeInstances")
        per_rule = defaultdict(float)
        groups = defaultdict(lambda: [0, 0.0, 0.0])  # (type, lifecycle) -> count, minutes, USD
        prices, used, available, running = {}, 0.0, 0.0, 0
        disk_prices, disk_usd, disks_known = {}, 0.0, True
        for iid, rules in self.vcpu_seconds.items():
            inst = found.get(iid)
            if not inst:
                return None, f"EC2 no longer knows instance {iid}"
            itype, az = inst["InstanceType"], inst.get("Placement", {}).get("AvailabilityZone")
            spot = inst.get("InstanceLifecycle") == "spot"
            start = max(_utc(inst["LaunchTime"]), self.started)
            m = _STOPPED_AT.search(inst.get("StateTransitionReason") or "")
            if inst.get("State", {}).get("Name") in ("running", "pending") or not m:
                end = now
                running += 1
            else:
                end = min(now, datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                          .replace(tzinfo=datetime.timezone.utc))
            hours = max(0.0, (end - start).total_seconds()) / 3600
            try:
                if itype not in prices:
                    prices[itype] = self._on_demand(pricing, itype)
                hourly, vcpus = prices[itype]
            except Exception as e:
                return None, _why(e, "pricing:GetProducts")
            if spot:
                try:
                    hourly = self._spot(itype, az, start, end)
                except Exception as e:
                    return None, _why(e, "ec2:DescribeSpotPriceHistory")
            cost = hourly * hours
            vols = self.volumes.get(iid)
            if vols is None:
                disks_known = False
            else:
                for gib, vtype in vols:
                    try:
                        if vtype not in disk_prices:
                            disk_prices[vtype] = self._volume_price(pricing, vtype)
                        extra = gib * disk_prices[vtype] / 730 * hours  # per GB-month, 730 h a month
                        cost += extra
                        disk_usd += extra
                    except Exception:
                        disks_known = False
            total = sum(rules.values())
            for rule, secs in rules.items():
                per_rule[rule] += cost * secs / total if total else 0
            g = groups[(itype, "Spot" if spot else "on-demand")]
            g[0] += 1
            g[1] += hours * 60
            g[2] += cost
            used += total
            available += vcpus * hours * 3600
        parts = [f"{n} × {t} ({kind}, {mins:.0f} min in all)" for (t, kind), (n, mins, _c) in sorted(groups.items())]
        total_usd = sum(g[2] for g in groups.values())
        usd = "<$0.01" if 0 < total_usd < 0.005 else f"~${total_usd:.2f}"
        disks = (f" (disks included: ${disk_usd:.2f})" if disks_known and disk_usd >= 0.005 else
                 " (disks included)" if disks_known else " (without their disks)")
        line = (f"EC2: {', '.join(parts)}: {usd} at AWS's prices{disks}"
                + (f"; the jobs used {used / available:.0%} of these instances' vCPU-time" if available else "")
                + (f" ({running} still running, counted until now)" if running else "") + ".")
        return dict(per_rule), line
