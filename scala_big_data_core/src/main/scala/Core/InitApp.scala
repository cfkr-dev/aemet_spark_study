package Core

import Core.DataExtraction.Aemet.AemetAPIClient
import Core.DataExtraction.Ifapa.{IfapaAPIClient, IfapaToAemetConverter}
import Core.PlotGeneration.PlotGenerator
import Core.Spark.SparkManager.SparkQueries
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{ListBucketsRequest, PutObjectRequest, S3Exception}

import java.net.URI
import java.nio.file.Paths

object InitApp extends App {
  AemetAPIClient.aemetDataExtraction()
  //IfapaAPIClient.ifapaDataExtraction()
  //IfapaToAemetConverter.ifapaToAemetConversion()
  //SparkQueries.execute()
  //PlotGenerator.generate()

//  val credentials = AwsBasicCredentials.create("test", "test")
//
//  val s3Client = S3Client.builder()
//    .endpointOverride(new URI("http://localhost:4566")) // LocalStack
//    .region(Region.US_EAST_1)
//    .credentialsProvider(StaticCredentialsProvider.create(credentials))
//    .forcePathStyle(true) // IMPORTANTÍSIMO para LocalStack
//    .build()
//
//  try {
//    // Listar buckets
//    val buckets = s3Client.listBuckets(ListBucketsRequest.builder().build())
//    println("Buckets existentes:")
//    buckets.buckets().forEach(b => println(s"- ${b.name()}"))
//
//    // === SUBIR ARCHIVO ===
//    val bucketName = "testbucket"
//    val filePath = Paths.get("C:/Users/alber/Desktop/sanchismo.jpg")
//    val key = "sanchismo.jpg"
//
//    val putRequest = PutObjectRequest.builder()
//      .bucket(bucketName)
//      .key(key)
//      .build()
//
//    s3Client.putObject(putRequest, filePath)
//
//    println(s"\nArchivo subido correctamente a s3://$bucketName/$key")
//
//  } catch {
//    case e: S3Exception =>
//      println(s"Error S3: ${e.awsErrorDetails().errorMessage()}")
//  } finally {
//    s3Client.close()
//  }


}
