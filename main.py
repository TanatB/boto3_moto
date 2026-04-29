import boto3
from moto import mock_aws, ThreadedMotoServer

class MyModel:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def save(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.put_object(Bucket="mybucket", Key=self.name, Body=self.value)


@mock_aws
def test_my_model_save():
    mock = mock_aws()
    mock.start()

    conn = boto3.resource("s3", region_name="us_east-1")
    # Create the bucket since everything is in Moto's Virtual AWS
    conn.create_bucket(Bucket="mybucket")

    model_instance = MyModel("tanat", "metmaolee")
    model_instance.save()

    body = conn.Object("mybucket", "tanat").get()[
        "Body"].read().decode("utf-8")
    
    assert body == "metmaolee"

    mock.stop()


def main():
    print("Hello from moto-boto3!")


if __name__ == "__main__":
    main()
