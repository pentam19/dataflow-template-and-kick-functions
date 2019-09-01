package com.sample;

/**
 * Hello world!
 *
 */
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

//$ mvn compile
//$ mvn exec:java -Dexec.mainClass=com.sample.App -Dexec.args="--templateLocation=gs://[bucket]/templates/mytemplate"
public class App {
    private static final String PROJECT = "[ProjectID]";
    private static final String BUCKET = "[Bucket]";
    private static final String STAGING_LOCATION = "gs://"+BUCKET+"/staging";
    private static final String TEMP_LOCATION = "gs://"+BUCKET+"/temp";
    private static final String TEMPLATE_LOCATION = "gs://"+BUCKET+"/templates/mytemplate";
    private static final String SRC_TOPIC_NAME = "dataflow-input";
    private static final String DST_TOPIC_NAME = "dataflow-output";
    private static final String SRC_PUBSUB_TOPIC = "projects/"+PROJECT+"/topics/"+SRC_TOPIC_NAME;
    private static final String DST_PUBSUB_TOPIC = "projects/"+PROJECT+"/topics/"+DST_TOPIC_NAME;

    static class MyFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output("Hello," + c.element());
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        //DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        //https://stackoverflow.com/questions/55714001/unable-to-create-a-google-cloud-dataflow-template-through-cli-command-using-java
        DataflowPipelineOptions dataflowOptions =
        PipelineOptionsFactory.fromArgs(args)
           .withValidation()
           .as(DataflowPipelineOptions.class);

        dataflowOptions.setRunner(DataflowRunner.class);
        dataflowOptions.setProject(PROJECT);
        dataflowOptions.setStagingLocation(STAGING_LOCATION);
        dataflowOptions.setTempLocation(TEMP_LOCATION);
        dataflowOptions.setNumWorkers(1);
        //dataflowOptions.setTemplateLocation(TEMPLATE_LOCATION);

        Pipeline p = Pipeline.create(dataflowOptions);
        p.apply(PubsubIO.readStrings().fromTopic(SRC_PUBSUB_TOPIC))
                .apply(ParDo.of(new MyFn()))
                .apply(PubsubIO.writeStrings().to(DST_PUBSUB_TOPIC));
        p.run();

        // Pull
        // $ gcloud pubsub subscriptions pull projects/[ProjectID]/subscriptions/[SubName] --auto-ack
    }
}
