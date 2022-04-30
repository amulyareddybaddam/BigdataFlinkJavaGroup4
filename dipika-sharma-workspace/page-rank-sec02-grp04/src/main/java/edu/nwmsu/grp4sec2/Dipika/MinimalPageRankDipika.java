package edu.nwmsu.grp4sec2.Dipika;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.*;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

public class MinimalPageRankDipika {

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

    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      int votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoters();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPage>) voters).size();
      }
      for (VotingPage vp : voters) {
        String pageName = vp.getVoterName();
        double pageRank = vp.getPageRank();
        String contributingPageName = element.getKey();
        double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, votes, contributingPageRank);
        ArrayList<VotingPage> arr = new ArrayList<>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getVoterName(), new RankedPage(pageName, pageRank, arr)));
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
    OutputReceiver<KV<String, RankedPage>> receiver) {
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPage> newVoters = new ArrayList<>();
      for(RankedPage rankPage:element.getValue()){
        if (rankPage != null) {
          for(VotingPage votingPage:rankPage.getVoters()){
            newVoters.add(votingPage);
            updatedRank += (dampingFactor) * votingPage.getPageRank() / (double)votingPage.getContributorVotes();
          }
        }
      }
      receiver.output(KV.of(element.getKey(),new RankedPage(element.getKey(), updatedRank, newVoters)));

  }
  }

  static class Job3 extends DoFn<KV<String, RankedPage>, KV<Double, String>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<Double, String>> receiver) {
      receiver.output(KV.of(element.getValue().getRank(), element.getKey()));
    }
  }

  public static void main(String[] args) {
    deleteFiles();

    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);

    String datFolder = "web04";
   // String file = "go.md";
    PCollection<KV<String, String>> pCol1 = DipikaKVPairMapper(p, "go.md", datFolder);
    PCollection<KV<String, String>> pCol2 = DipikaKVPairMapper(p, "python.md", datFolder);
    PCollection<KV<String, String>> pCol3 = DipikaKVPairMapper(p, "java.md", datFolder);
    PCollection<KV<String, String>> pCol4 = DipikaKVPairMapper(p, "README.md", datFolder);

    PCollectionList<KV<String, String>> pColList = PCollectionList.of(pCol1).and(pCol2).and(pCol3)
        .and(pCol4);
    PCollection<KV<String, String>> list = pColList.apply(Flatten.<KV<String, String>>pCollections());

    // Group by Key to get a single record for each page
    PCollection<KV<String, Iterable<String>>> grouped = list.apply(GroupByKey.<String, String>create());
    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = grouped.apply(ParDo.of(new Job1Finalizer()));

    // PCollection<String> pColLinkString =
    // job2in.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    PCollection<KV<String, RankedPage>> job2out = null;
    int iterations = 35;
    for (int i = 1; i <= iterations; i++) {

      PCollection<KV<String, RankedPage>> job2Mapper = job2in.apply(ParDo.of(new Job2Mapper()));

      PCollection<KV<String, Iterable<RankedPage>>> job2MapperGrpByKey = job2Mapper.apply(GroupByKey.create());

      job2out = job2MapperGrpByKey.apply(ParDo.of(new Job2Updater()));
      // update job2in so it equals the new job2out
      job2in = job2out;
    }
    PCollection<KV<Double, String>> job3 = job2out.apply(ParDo.of(new Job3()));

    // Get the maximum value using Combine transform which required comparator
    // implemented class instance as a parameter
    PCollection<KV<Double, String>> maxRank = job3.apply(Combine.globally(Max.of(new RankedPage())));

    PCollection<String> pColLinksString = maxRank
        .apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut) -> mergeOut.toString()));
    pColLinksString.apply(TextIO.write().to("Output_Dipika"));
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String, String>> DipikaKVPairMapper(Pipeline p, String filename, String folder) {

    String path = folder + "/" + filename;

    PCollection<String> pcolInput = p.apply(TextIO.read().from(path));

    PCollection<String> plink = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));

    PCollection<String> pcollectionLinks = plink.apply(MapElements
        .into((TypeDescriptors.strings()))
        .via((String linkLine) -> linkLine.substring(linkLine.indexOf("(") + 1, linkLine.length() - 1)));

    PCollection<KV<String, String>> pColKVPairs = pcollectionLinks
        .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via((String outLink) -> KV.of(filename, outLink)));

    return pColKVPairs;
  }


  private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {

    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());

      PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

    return updatedOutput;
  }

  public static void deleteFiles() {
    final File file = new File("./");
    for (File f : file.listFiles()) {
      if (f.getName().startsWith("Output_Dipika")) {
        f.delete();
      }
    }
  }

}
