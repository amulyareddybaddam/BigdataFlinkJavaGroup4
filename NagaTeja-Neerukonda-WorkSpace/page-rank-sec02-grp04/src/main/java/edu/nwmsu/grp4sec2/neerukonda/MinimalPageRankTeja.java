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
    PCollection<String> pColLinksString = list.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    pColLinksString.apply(TextIO.write().to("Output_Teja"));  
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> TejaKVPairMapper(Pipeline p, String filename, String folder){
   
    String path = folder + "/" + filename;
     PCollection<String> pcolInput = p.apply(TextIO.read().from(path));
     PCollection<String> plink = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
     PCollection<String> pcollectionLinks = plink.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
     PCollection<KV<String,String>> pColKVPairs =  pcollectionLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(filename,outLink)));
    return pColKVPairs;
  }

   // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
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

}


