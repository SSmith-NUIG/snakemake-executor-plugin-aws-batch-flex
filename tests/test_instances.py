"""Pricing the EC2 instances a run used (instances.py), with stub AWS clients."""
import datetime, json

from snakemake_executor_plugin_aws_batch_flex.instances import Instances

T0 = datetime.datetime(2026, 10, 23, 12, 0, tzinfo=datetime.timezone.utc)


def ms(minutes):
    return int((T0 + datetime.timedelta(minutes=minutes)).timestamp() * 1000)


class ECS:
    def describe_container_instances(self, containerInstances, cluster=None):
        assert cluster == "c1"
        return {"containerInstances": [{"ec2InstanceId": "i-" + a.rsplit("/", 1)[-1]} for a in containerInstances]}


class EC2:
    def describe_instances(self, InstanceIds):
        insts = {
            # on-demand, ran 12:00-13:00 and was shut down
            "i-a": {"InstanceType": "m6i.2xlarge", "LaunchTime": T0, "State": {"Name": "terminated"},
                    "StateTransitionReason": "User initiated (2026-10-23 13:00:00 GMT)",
                    "Placement": {"AvailabilityZone": "eu-west-1a"}},
            # Spot, started 12:30 and still running at 13:00
            "i-b": {"InstanceType": "m6i.2xlarge", "LaunchTime": T0 + datetime.timedelta(minutes=30),
                    "State": {"Name": "running"}, "InstanceLifecycle": "spot",
                    "Placement": {"AvailabilityZone": "eu-west-1b"}},
        }
        return {"Reservations": [{"Instances": [
            dict(insts[i], InstanceId=i, BlockDeviceMappings=[{"Ebs": {"VolumeId": "vol-" + i}}])
            for i in InstanceIds]}]}

    def describe_volumes(self, VolumeIds):
        return {"Volumes": [{"VolumeId": v, "Size": 100, "VolumeType": "gp3"} for v in VolumeIds]}

    def describe_spot_price_history(self, **kw):
        assert kw["AvailabilityZone"] == "eu-west-1b"
        return {"SpotPriceHistory": [{"SpotPrice": "0.10"}, {"SpotPrice": "0.14"}]}


class Pricing:
    def get_products(self, ServiceCode, Filters, MaxResults):
        if {"Type": "TERM_MATCH", "Field": "productFamily", "Value": "Storage"} in Filters:
            p = {"terms": {"OnDemand": {"t": {"priceDimensions": {"d": {"pricePerUnit": {"USD": "0.073"}}}}}}}
            return {"PriceList": [json.dumps(p)]}  # gp3: $0.073 per GB-month -> 100 GiB = $0.01/h
        assert {"Type": "TERM_MATCH", "Field": "instanceType", "Value": "m6i.2xlarge"} in Filters
        p = {"product": {"attributes": {"vcpu": "8"}},
             "terms": {"OnDemand": {"t": {"priceDimensions": {"d": {"pricePerUnit": {"USD": "0.428"}}}}}}}
        return {"PriceList": [json.dumps(p)]}


def job(arn, start, stop):
    return {"container": {"containerInstanceArn": arn}, "startedAt": ms(start), "stoppedAt": ms(stop)}


def test_instances_are_priced_and_shared_out_by_vcpu_time():
    inst = Instances(ECS(), EC2(), "eu-west-1", started=T0)
    a = "arn:aws:ecs:eu-west-1:1:container-instance/c1/a"
    b = "arn:aws:ecs:eu-west-1:1:container-instance/c1/b"
    # on i-a: align uses 4 vCPUs for 45 min, sort 2 vCPUs for 45 min (2:1 vCPU-time)
    assert inst.add_job("align", job(a, 5, 50), 4)
    assert inst.add_job("sort", job(a, 5, 50), 2)
    assert inst.add_job("sort", job(b, 35, 55), 2)
    per_rule, line = inst.costs(Pricing(), now=T0 + datetime.timedelta(minutes=60))
    # i-a: 1 h on-demand at 0.428 + its 100 GiB disk at 0.01 -> align 0.292, sort 0.146;
    # i-b: 0.5 h Spot at 0.12 + disk 0.005 -> sort 0.065
    assert round(per_rule["align"], 3) == 0.292 and round(per_rule["sort"], 3) == 0.211
    assert "1 × m6i.2xlarge (Spot, 30 min in all)" in line and "1 × m6i.2xlarge (on-demand, 60 min in all)" in line
    assert "~$0.50" in line and "(disks included: $0.01)" in line and "1 still running" in line
    # vCPU-time used: 4*45 + 2*45 + 2*20 = 310 vCPU-min of 8*90 = 720 available
    assert "43% of these instances' vCPU-time" in line


def test_run_start_clips_an_instance_started_earlier():
    inst = Instances(ECS(), EC2(), "eu-west-1", started=T0 + datetime.timedelta(minutes=30))
    inst.add_job("x", job("arn:aws:ecs:eu-west-1:1:container-instance/c1/a", 35, 50), 8)
    per_rule, line = inst.costs(Pricing(), now=T0 + datetime.timedelta(minutes=60))
    assert round(per_rule["x"], 3) == 0.219  # only 12:30-13:00 of i-a counts for this run


def test_unknown_instance_and_failing_lookups():
    class NoECS:
        def describe_container_instances(self, **kw):
            raise PermissionError("AccessDenied")
    inst = Instances(NoECS(), EC2(), "eu-west-1", started=T0)
    assert not inst.add_job("x", job("arn:aws:ecs:eu-west-1:1:container-instance/c1/a", 0, 10), 1)
    per_rule, why = inst.costs(Pricing())
    assert per_rule is None and why == "no permission for ecs:DescribeContainerInstances"

    class NoPricing:
        def get_products(self, **kw):
            raise PermissionError("AccessDenied")
    inst = Instances(ECS(), EC2(), "eu-west-1", started=T0)
    inst.add_job("x", job("arn:aws:ecs:eu-west-1:1:container-instance/c1/a", 0, 10), 1)
    per_rule, why = inst.costs(NoPricing(), now=T0 + datetime.timedelta(minutes=60))
    assert per_rule is None and why == "no permission for pricing:GetProducts"


def test_table_uses_instance_costs_or_falls_back():
    from snakemake_executor_plugin_aws_batch_flex.costs import Usage
    u = Usage()
    d = {"startedAt": 0, "stoppedAt": 3600 * 1000}
    u.add("align", d, 4, 4096, "ec2", False, on_known_instance=True)
    u.add("qc", d, 1, 2048, "fargate", False)
    t = u.table(256, {"align": 0.5}, "EC2: 1 × m6i.2xlarge (on-demand, 60 min in all): ~$0.50.")
    assert "the EC2 instances at AWS's prices" in t and "EC2: 1 × m6i.2xlarge" in t
    align = next(l for l in t.splitlines() if l.strip().startswith("align")).split()
    assert align[4] == "0.50"
    t = u.table(256, None, "EC2 cost estimated from the jobs' vCPUs (...)")
    align = next(l for l in t.splitlines() if l.strip().startswith("align")).split()
    assert align[4] == "0.20" and "rough estimate" in t  # 4 vCPU-h at $0.05


def test_instance_priced_without_its_disk_when_volumes_are_unknown():
    class EC2NoVolumes(EC2):
        def describe_volumes(self, VolumeIds):
            raise PermissionError("AccessDenied")
    inst = Instances(ECS(), EC2NoVolumes(), "eu-west-1", started=T0)
    inst.add_job("x", job("arn:aws:ecs:eu-west-1:1:container-instance/c1/a", 0, 60), 8)
    per_rule, line = inst.costs(Pricing(), now=T0 + datetime.timedelta(minutes=60))
    assert round(per_rule["x"], 3) == 0.428 and "(without their disks)" in line
