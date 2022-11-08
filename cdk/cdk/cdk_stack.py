from typing import List

from aws_cdk import (
    Duration,
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_emrserverless as emrs,
)
from constructs import Construct


class CdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        github_repo = self.node.try_get_context("repo_pattern")
        if github_repo is None:
            raise ValueError("repo_pattern context variable is required: <user|org>/<repo>")

        # Create a bucket for test artifacts and a "production" bucket
        test_bucket = s3.Bucket(
            self,
            "TestBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        prod_bucket = s3.Bucket(
            self,
            "ProdBucket",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        CfnOutput(self, "s3_test_bucket", value=test_bucket.bucket_name)
        CfnOutput(self, "s3_prod_bucket", value=prod_bucket.bucket_name)

        # We create two EMR Serverless applications. One for test and one for production.
        # The test environment is fairly basic, doesn't need any special network access or pre-init capacity.
        test_serverless_app = emrs.CfnApplication(
            self,
            "SparkTestEnvironment",
            release_label="emr-6.7.0",
            type="SPARK",
            name="ghuni-test",
            initial_capacity=[
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=2,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=10,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
            ],
            auto_stop_configuration=emrs.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=100
            ),
        )
        # The prod environment has 2 pre-init drivers and doesn't auto-stop for 30 minutes.
        # This is because we run jobs every 15 minutes and we want them to run asap.
        prod_serverless_app = emrs.CfnApplication(
            self,
            "SparkProdEnvrionment",
            release_label="emr-6.7.0",
            type="SPARK",
            name="ghuni-prod",
            initial_capacity=[
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Driver",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=2,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="Executor",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=10,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU", memory="16gb"
                        ),
                    ),
                ),
            ],
            auto_stop_configuration=emrs.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=100
            ),
        )
        CfnOutput(
            self,
            "emr_serverless_test_appid",
            value=test_serverless_app.attr_application_id,
        )
        CfnOutput(
            self,
            "emr_serverless_prod_appid",
            value=prod_serverless_app.attr_application_id,
        )

        # We create a single IAM role that can access both our test and prod bucket.
        # We normally wouldn't do that in a production environment.
        emr_serverless_job_role = iam.Role(
            self,
            "EMRServerlessJobRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            inline_policies={
                "S3Access": self.s3_access_policy([test_bucket, prod_bucket]),
                "GlueAccess": self.glue_access_policy(),
            },
        )

        # We also need a role GitHub OIDC so we can execute AWS commands from our Actions
        # NOTE: You can only have one of these providers per URL per AWS account
        github_provider = iam.OpenIdConnectProvider(
            self,
            "GitHubProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
        )
        # Does this create the thumbprint for us?
        github_oidc_role = iam.Role(
            self,
            "GitHubOIDCRole",
            assumed_by=iam.WebIdentityPrincipal(
                github_provider.open_id_connect_provider_arn,
                conditions={
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub": "repo:dacort/*:*"
                    },
                    "ForAllValues:StringEquals": {
                        "token.actions.githubusercontent.com:iss": "https://token.actions.githubusercontent.com",
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                    },
                },
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonEC2ContainerRegistryReadOnly"
                )
            ],
            description="This role is used via GitHub Actions to deploy with AWS CDK or Terraform on the target AWS account",
            max_session_duration=Duration.hours(1),
            inline_policies={
                "EMRServerlessAccess": self.emr_serverless_start_and_run_policy(
                    emr_serverless_job_role, [test_serverless_app, prod_serverless_app]
                ),
                "S3Access": self.s3_access_policy([test_bucket, prod_bucket]),
            },
        )

        CfnOutput(
            self, "emr_serverless_job_role", value=emr_serverless_job_role.role_arn
        )
        CfnOutput(self, "github_oidc_role", value=github_oidc_role.role_arn)

    def s3_access_policy(self, buckets: List[s3.Bucket]):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=["s3:GetObject", "s3:ListBucket"],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["s3:PutObject", "s3:DeleteObject"],
                    effect=iam.Effect.ALLOW,
                    resources=[f"{bucket.bucket_arn}/*" for bucket in buckets],
                ),
            ]
        )

    def glue_access_policy(self):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "glue:GetDatabase",
                        "glue:GetDataBases",
                        "glue:CreateTable",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:CreatePartition",
                        "glue:BatchCreatePartition",
                        "glue:GetUserDefinedFunctions",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
            ]
        )

    def emr_serverless_start_and_run_policy(
        self, emr_serverless_job_role: iam.Role, applications: List[emrs.CfnApplication]
    ):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "emr-serverless:StartApplication",
                        "emr-serverless:DescribeApplication",
                        "emr-serverless:StartJobRun",
                        "emr-serverless:GetJobRun",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[a.attr_arn for a in applications],
                ),
                iam.PolicyStatement(
                    actions=[
                        "emr-serverless:GetJobRun",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"{a.attr_arn}/jobruns/*" for a in applications],
                ),
                iam.PolicyStatement(
                    actions=[
                        "iam:PassRole",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[emr_serverless_job_role.role_arn],
                    conditions={
                        "StringLike": {
                            "iam:PassedToService": "emr-serverless.amazonaws.com"
                        }
                    },
                ),
            ]
        )
