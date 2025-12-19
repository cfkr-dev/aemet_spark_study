# Amazon Web Services (AWS) Deployment for the Spark App Pipeline

---

## 1. Setup

### 1.1 Start AWS Lab Session

* Start a Lab session in **AWS Academy**.

### 1.2 Create S3 Bucket

* Create a bucket to store your data and model artifacts:

```bash
aws s3 mb s3://aperezpe2018-meteo-study-bucket
```

### 1.3 Upload Files to S3

* Upload the JAR file, config files, and data from the `data-extraction` process:

```bash
aws s3 cp ./big-data-core/deployment/spark-app/spark-app-cluster-1.0.0.jar s3://aperezpe2018-meteo-study-bucket/
aws s3 cp --recursive ./data/config s3://aperezpe2018-meteo-study-bucket/data/config/
aws s3 cp --recursive ./data/data_extraction/aemet_spark_format s3://aperezpe2018-meteo-study-bucket/data/data_extraction/aemet_spark_format/
aws s3 cp --recursive ./data/data_extraction/ifapa_spark_format s3://aperezpe2018-meteo-study-bucket/data/data_extraction/ifapa_spark_format/
```

---

## 2. Create EMR Cluster

1. Go to **AWS EMR Service** and click on **Create cluster**.

2. Configure cluster settings:

    * **Name:** `meteo-study-spark-cluster`
    * **EMR Version:** `emr-7.9.0`
    * **Applications:** `Spark Interactive`
    * **Instance Types:**

        * Master: `m4.large`
        * Core: `m4.large`
        * Task: `m4.large`
    * **Roles:**

        * Service Role: `EMR_DefaultRole`
        * EC2 Role: `EMR_EC2_DefaultRole`
    * **Log Storage:** `s3://aperezpe2018-meteo-study-bucket/logs/`

3. Set **Software Settings**:

```json
[
  {
    "Classification": "spark-env",
    "Properties": {},
    "Configurations": [
      {
        "Classification": "export",
        "Properties": {
          "STORAGE_PREFIX": "s3://aperezpe2018-meteo-study-bucket",
          "STORAGE_BASE": "/data"
        }
      }
    ]
  }
]

```

4. Click **Create cluster** and wait until the cluster state is **Waiting**.

---

## 3. Add Spark Step

1. Create a step to run the Spark job:

    * **Step Type:** Spark application
    * **Name:** `meteo-spark-step`
    * **Deploy Mode:** cluster
    * **Application Location:** `s3://aperezpe2018-meteo-study-bucket/spark-app-cluster-1.0.0.jar`
    * **Spark-Submit Arguments:**

```bash
--master yarn --class Spark.Main
```

* **Action on Failure:** Continue

2. Click **Add**.
3. Wait until the step state is **Completed**.
