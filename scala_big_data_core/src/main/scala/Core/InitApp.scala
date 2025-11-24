package Core

import Core.DataExtraction.Aemet.AemetAPIClient
import Core.DataExtraction.Ifapa.{IfapaAPIClient, IfapaToAemetConverter}
import Core.PlotGeneration.PlotGenerator
import Core.Spark.SparkManager.SparkQueries
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, ListBucketsRequest, PutObjectRequest, S3Exception}

import java.net.URI
import java.nio.file.Paths

object InitApp extends App {
  AemetAPIClient.aemetDataExtraction()
  IfapaAPIClient.ifapaDataExtraction()
  IfapaToAemetConverter.ifapaToAemetConversion()
  SparkQueries.execute()
  //PlotGenerator.generate()
}
