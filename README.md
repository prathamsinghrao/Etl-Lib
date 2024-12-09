# **EtlFlow: A Lightweight ETL Framework for .NET**

**EtlFlow** is a compact and efficient ETL (Extract-Transform-Load) framework designed for .NET Standard. Its goal is to streamline simple ETL tasks with a clear, declarative approach. Ideal for less complex data transformation and loading tasks, EtlFlow emphasizes clarity, maintainability, and extensibility. For more demanding, complex ETL workflows involving large-scale transformations or integrations, you might want to explore full-featured solutions like Pentaho or Apache Nifi.

### **Project Objectives:**
- Offer an easy-to-understand, declarative DSL that promotes clean and review-friendly code.
- Provide reusable components for frequent ETL activities like file handling, data manipulation, and database communication.
- Ensure solid performance even for moderate-sized data loads.
- Build a flexible architecture that allows users to extend functionality through integration modules.
- Support troubleshooting and error diagnostics to streamline development.

### **Current State:**

The project is in the **beta** stage. While it has been used in several smaller projects, there are still areas requiring further testing to ensure complete stability and high test coverage.

### **Available Components:**

| **Package**                  | **Description**                                                | **Status**     |
| ---------------------------- | -------------------------------------------------------------- | :------------: |
| **EtlFlow**                  | Core library with foundational ETL functionality.              | Mostly Tested  |
| **EtlFlow.Logging.NLog**      | Integration with NLog for logging and debugging.               | OK             |
| **EtlFlow.Nodes.S3**          | Integration with Amazon S3 for data storage.                   | OK             |
| **EtlFlow.Nodes.Csv**         | Uses CsvHelper to handle CSV file reading and writing.         | OK             |
| **EtlFlow.Nodes.Dapper**      | Integration with Dapper for lightweight database access.       | Mostly Tested  |
| **EtlFlow.Nodes.Compression** | Uses SharpZipLib to compress files during processing.          | Mostly Tested  |
| **EtlFlow.Nodes.MongoDB**     | Integration with MongoDB to handle document-based storage.     | Not Tested     |
| **EtlFlow.Nodes.PostgreSQL**  | PostgreSQL integration via Npgsql driver for data reading/writing. | Not Tested     |
| **EtlFlow.Nodes.Redshift**    | Redshift integration for large-scale data migration.           | Not Tested     |
| **EtlFlow.Nodes.SqlServer**   | Integration with SQL Server for handling relational data.      | Not Tested     |
| **EtlFlow.Nodes.SFTP**        | Integration with SSH.NET for SFTP file transfer.               | Not Tested     |

---

### **Quick Example:**

In this example, an ETL process reads a CSV file, classifies data into income segments, filters out certain rows, adds new attributes, and outputs the result into a new CSV file while compressing the output.

```csharp
EtlFlowConfig.LoggingAdapter = new NLogLoggingAdapter();

var processBuilder = EtlProcessBuilder.Create()
    .Input(ctx => new CsvReaderNode(@"C:\Data\LoanStats.csv"))
    .GenerateRowNumbers("_id")
    .Classify("income_segment", category =>
    {
        decimal CalculateIncome(Row row) => row.GetAs<decimal>("annual_inc");
        category
          .When(x => string.IsNullOrWhiteSpace(x.GetAs<string>("annual_inc")), "UNKNOWN")
          .When(x => CalculateIncome(x) < 10000, "0-9999")
          .When(x => CalculateIncome(x) < 20000, "10000-19999")
          .When(x => CalculateIncome(x) < 30000, "20000-29999")
          .Default("30000+");
    })
    .Filter(row => row.GetAs<string>("grade") != "A")
    .Transform((ctx, row) =>
    {
        var newRow = ctx.ObjectPool.Borrow<Row>();
        row.CopyTo(newRow);
        newRow["is_transformed"] = true;
        ctx.ObjectPool.Return(row);
        return newRow;
    })
    .Continue(ctx => new CsvWriterNode(@"C:\Data\LoanStats_Transformed.csv"))
        .IncludeHeader()
        .WithEncoding(Encoding.UTF8))
    .BZip2Files()
    .CompleteWithResult();
```

In this example, the methods `GenerateRowNumbers()`, `Classify()`, `Filter()`, and `Transform()` offer cleaner code by encapsulating transformation logic into simple operations. The `BZip2Files()` extension method compresses the output for efficiency.

---

### **Core Concepts in EtlFlow:**

#### **ETL Pipeline**
The main structure in EtlFlow is the **ETL Pipeline**, which consists of a series of operations performed on your data. By default, these operations are executed sequentially, but you can also opt to run them in parallel using the `.RunParallel()` method to improve performance in suitable use cases.

#### **Pipeline Context**
Each pipeline has an associated **Pipeline Context**, which holds configuration parameters, state, and any other contextual data needed during the ETL process. The context is thread-safe and can be passed through various stages of the pipeline.

##### Example: Declarative Configuration

```csharp
var config = new EtlPipelineConfig()
    .Set("s3_bucket", "my-bucket")
    .SetAmazonS3Credentials("accessKey", "secretKey")
    .Set("output_directory", @"C:\Output\");

var context = new EtlPipelineContext(config);
context.State["username"] = "user1";

EtlPipeline.Create(context)...
```

##### Example: Inline Configuration

```csharp
EtlPipeline.Create(settings =>
{
    settings.WithConfig(cfg => cfg
        .Set("s3_bucket", "my-bucket")
        .SetAmazonS3Credentials("accessKey", "secretKey")
        .Set("output_directory", @"C:\Output\"))

    settings.WithContextInitializer(ctx => ctx.State["username"] = "user1");
})...
```

#### **Executing the Pipeline**

EtlFlow supports lazy execution of pipelines. You can define your pipeline but choose when to execute it, allowing for flexible handling of data processing.

```csharp
var pipeline = EtlPipeline.Create(settings => {})
    .Run(ctx => new ActionEtlOperation(() => Console.WriteLine("Starting ETL...")));

var result = pipeline.Execute();
```

Or you can chain operations for cleaner code:

```csharp
var result = EtlPipeline.Create(settings => {})
    .Run(ctx => new ActionEtlOperation(() => Console.WriteLine("Processing...")))
    .Execute();
```

#### **ETL Operations**

**ETL Operations** form the building blocks of your ETL process. There are three types:

- **Void Operations**: These perform an action but don't return a result, e.g., logging or sending data to an external service.
- **Scalar Operations**: These return a single result, like a boolean or integer, after performing a task.
- **Enumerable Operations**: These return a collection of results, such as a list of files or records processed.

```csharp
EtlPipeline.Create(settings => {})
    .Run(ctx => new ActionEtlOperation(() => Console.WriteLine("Starting operation")))
    .RunParallel(ctx => new[]
    {
        new ActionEtlOperation(() => Console.WriteLine("Task 1")),
        new ActionEtlOperation(() => Console.WriteLine("Task 2"))
    });
```

#### **ETL Processes**

ETL Processes are specialized ETL Operations designed for more complex transformations. These processes allow for streaming-style transformations, reducing memory consumption when working with large datasets.

```csharp
public class GenerateDateDimensionEtlProcess : AbstractEtlProcess<NodeOutputWithFilePath>
{
    private static readonly Calendar Calendar = new GregorianCalendar();

    public GenerateDateDimensionEtlProcess(string outputPath)
    {
        Build(builder =>
        {
            builder
                .GenerateInput<Row, DateTime>(gen => gen.State <= endDate,
                (ctx, i, gen) =>
                {
                    if (i == 1) gen.SetState(startDate);
                    var row = ctx.ObjectPool.Borrow<Row>();
                    CreateRowFromDateTime(row, gen.State);
                    gen.SetState(gen.State.AddDays(1));
                    return row;
                })
                .Continue(ctx => new CsvWriterNode(outputPath).IncludeHeader())
                .BZip2Files(cfg => cfg.CompressionLevel(9).FileSuffix(".bz2"))
                .CompleteWithResult();
        });
    }
}
```

#### **Node Processing**

**Nodes** in EtlFlow process data in a streaming manner. Each node runs in its own thread and communicates with others via input/output adapters. Data is passed through as immutable objects, which can be cloned or reset as necessary to ensure smooth processing.

### **Logging**

EtlFlow integrates easily with popular logging frameworks, such as NLog, to provide insightful logs throughout the ETL process. By setting a logging adapter, you can capture detailed logs of your ETL jobs.

```csharp
EtlFlowConfig.LoggingAdapter = new NLogLoggingAdapter();
```

---

### **What's Next?**

We are continuously working on enhancing EtlFlow, with upcoming features and further integrations for additional data sources and storage options. Stay tuned for new updates!
"# Etl-Lib" 
"# Etl-Lib" 
