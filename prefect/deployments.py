import myflow
import pendulum
import datetime
from prefect.infrastructure import docker
from prefect.deployments import Deployment
from prefect_aws import S3Bucket
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

schedule = IntervalSchedule(
    anchor_date=pendulum.now(tz="Europe/Madrid"),
    interval=datetime.timedelta(hours=24)
)

docker.DockerRegistry(username=DOCKER_USER, password=DOCKER_PASSWORD, registry_url=DOCKER_REGISTRY)
dc = docker.DockerContainer(image="myprefect:1")
s3_storage =  S3Bucket.load("footbal-space-data-storage")

deployment_tfm = Deployment.build_from_flow(
    flow=myflow.etl_flow,
    name="football_data_space_prefect",
    version=1, 
    work_pool_name="football_data_space_prefect_managed",
    work_queue_name="default",
    infrastructure=dc,
    storage=s3_storage
)

deployment = deployment_tfm
deployment.apply()
