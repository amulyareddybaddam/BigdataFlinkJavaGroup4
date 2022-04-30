/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalPageRankSharma {

  public static void main(String[] args) {

    deleteOutputFiles();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";

    PCollection<KV<String, String>> pcol1 = mapInputFile(p, dataFolder, "go.md");
    PCollection<KV<String, String>> pcol2 = mapInputFile(p, dataFolder, "java.md");
    PCollection<KV<String, String>> pcol3 = mapInputFile(p, dataFolder, "python.md");
    PCollection<KV<String, String>> pcol4 = mapInputFile(p, dataFolder, "README.md");

    // List and Flatten the List to merge all input KV pairs
    PCollectionList<KV<String, String>> pcolList = PCollectionList.of(pcol1).and(pcol2).and(pcol3).and(pcol4);
    PCollection<KV<String, String>> mergedKV = pcolList.apply(Flatten.<KV<String, String>>pCollections());

    // Group by Key to get a single record for each page
    PCollection<KV<String, Iterable<String>>> kvStringReducedPairs = mergedKV
        .apply(GroupByKey.<String, String>create());

    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = kvStringReducedPairs.apply(ParDo.of(new Job1Finalizer()));

    // END JOB 1
    // ========================================
    // KV{python.md, python.md, 1.00000, 0, [README.md, 1.00000,1]}
    // KV{go.md, go.md, 1.00000, 0, [README.md, 1.00000,1]}
    // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,3, java.md, 1.00000,3,
    // python.md, 1.00000,3]}
    // ========================================
    // BEGIN ITERATIVE JOB 2

 // more work here.....

    // END ITERATIVE JOB 2
    // ========================================
    // after 40 - output might look like this:
    // KV{java.md, java.md, 0.69415, 0, [README.md, 1.92054,3]}
    // KV{python.md, python.md, 0.69415, 0, [README.md, 1.92054,3]}
    // KV{README.md, README.md, 1.91754, 0, [go.md, 0.69315,1, java.md, 0.69315,1,
    // python.md, 0.69315,1]}

    // Map KVs to strings before outputting
    PCollection<String> output = job2out.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));

    // Write from Beam back out into the real world
    output.apply(TextIO.write().to("rank"));

    p.run().waitUntilFinish();
  }

}
-----------------
/**
   * Process a single web page by itself.
   * Later we'll LIST & FLATTEN them to create a complete set of mapped pairs.
   *
   * @param p          - this pipeline
   * @param dataFolder - directory where the web pages are located
   * @param dataFile   - name of the web page to process
   * @return - returns a PCollection<KV<String, String>> each with (inputFileName,
   *         outLinkFileName)
   **/
  private static PCollection<KV<String, String>> mapInputFile(Pipeline p, String dataFolder, String dataFile) {
    String dataPath = dataFolder + "/" + dataFile;
    PCollection<String> inputlines = p.apply(TextIO.read().from(dataPath));
    PCollection<String> longlinklines = inputlines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> linklines = longlinklines
        .apply(MapElements.into(TypeDescriptors.strings()).via(linkline -> linkline.strip()));
    PCollection<String> linkpages = linklines.apply(MapElements.into(TypeDescriptors.strings()).via(
        linkline -> linkline.substring(linkline.indexOf('(') + 1, linkline.length() - 1)));
    PCollection<KV<String, String>> kvPairs = linkpages.apply(MapElements.into(
        TypeDescriptors.kvs(
            TypeDescriptors.strings(),
            TypeDescriptors.strings()))
        .via(
            linkpage -> KV.of(dataFile, linkpage)));
    return kvPairs;
  }
----------------
  /**
   * Tired of deleting output files manually?
   * Add a helper function to delete them before starting the pipeline.
   */
  private static void deleteOutputFiles() {
    final File dir = new File("./");
    for (File f : dir.listFiles()) {
      if (f.getName().startsWith("rank")) {
        f.delete();
      }
    }
  }