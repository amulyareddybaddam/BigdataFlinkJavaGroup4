package edu.nwmsu.grp4sec2.neerukonda;

import java.util.ArrayList;
import java.util.Collection;



import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.*;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

public class MinimalPageRankTeja {



static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }
  
  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }
  
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);

    String datFolder="web04";
    String file="go.md";
   PCollection<KV<String,String>> pColKVpair1 = TejaKVPairMapper(p,"go.md",datFolder);
   PCollection<KV<String,String>> pColKVpair2 = TejaKVPairMapper(p,"python.md",datFolder);
   PCollection<KV<String,String>> pColKVpair3 = TejaKVPairMapper(p,"java.md",datFolder);
   PCollection<KV<String,String>> pColKVpair4 = TejaKVPairMapper(p,"README.md",datFolder);


   
    PCollectionList<KV<String, String>> pColList = PCollectionList.of(pColKVpair1).and(pColKVpair2).and(pColKVpair3).and(pColKVpair4);
    PCollection<KV<String, String>> list = pColList.apply(Flatten.<KV<String,String>>pCollections());

        // Group by Key to get a single record for each page
    PCollection<KV<String, Iterable<String>>> grouped =list.apply(GroupByKey.<String, String>create());
        // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = grouped.apply(ParDo.of(new Job1Finalizer()));
 
    //PCollection<String> pColLinkString = job2in.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    PCollection<KV<String, RankedPage>> job2out = null; 
    int iterations = 2;
    for (int i = 1; i <= iterations; i++) {
      // use job2in to calculate job2 out
      // .... write code here
      // update job2in so it equals the new job2out
      // ... write code here
    }

    PCollection<String> pColLinksString = list.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    pColLinksString.apply(TextIO.write().to("Output_Teja"));  
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> TejaKVPairMapper(Pipeline p, String filename, String folder){
   
    String path = folder + "/" + filename;

     PCollection<String> pcolInput = p.apply(TextIO.read().from(path));

     PCollection<String> plink = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));

     PCollection<String> pcollectionLinks = plink.apply(MapElements
                        .into((TypeDescriptors.strings()))
                       .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));


     PCollection<KV<String,String>> pColKVPairs =  pcollectionLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                       .via((String outLink) -> KV.of(filename,outLink)));

    return pColKVPairs;
  }

    private static PCollection<KV<String, RankedPage>> runJob2Iteration(PCollection<KV<String, RankedPage>> kvReducedPairs) {

        PCollection<KV<String, RankedPage>> updatedOutput = null;
        return updatedOutput;
    }

    public static  void deleteFiles(){
      final File file = new File("./");
      for (File f : file.listFiles()){
        if(f.getName().startsWith("Output_Teja")){
          f.delete();
        }
      }
    }

}


