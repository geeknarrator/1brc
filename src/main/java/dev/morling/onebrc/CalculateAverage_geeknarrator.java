/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.lang.Math.round;


public class CalculateAverage_geeknarrator {

  private static final String FILE = "./measurements.txt";
  private static final long CHUNK_SIZE = 100_000_00;

  private static record Measurement(String station, double value) {
    private Measurement(String[] parts) {
      this(parts[0], Double.parseDouble(parts[1]));
    }
  }

  private static record ResultRow(double min, double mean, double max) {
    public String toString() {
      return round(min) + "/" + round(mean) + "/" + round(max);
    }

    private double round(double value) {
      return Math.round(value * 10.0) / 10.0;
    }
  };

  private static class MeasurementAggregator {
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double sum;
    private long count;

    public static MeasurementAggregator of(double min, double max, double sum, long count) {
      var res = new MeasurementAggregator();
      res.min = min;
      res.max = max;
      res.sum = sum;
      res.count = count;
      return res;
    }

    public static MeasurementAggregator merge(MeasurementAggregator m1, MeasurementAggregator m2) {
      return MeasurementAggregator.of(Math.min(m1.min, m2.min), Math.max(m1.max, m2.max),
          m1.sum + m2.sum,
          m1.count + m2.count
      );
    }
  }

  public static void main(String[] args) throws IOException {
    long start = System.currentTimeMillis();

    //readBuffered();
    System.out.println(
        processFile(FILE).entrySet().stream().sorted(Map.Entry.comparingByKey()).map(m -> {
          MeasurementAggregator value = m.getValue();
          return m.getKey()+"="+value.min+"/"+round(value.sum/value.count)+"/"+value.max;
        }).collect(Collectors.joining(", ")));


    System.out.println("Total time: " + (System.currentTimeMillis() - start) + "ms");
  }


  private static void readBuffered()
      throws IOException {
    ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    BufferedReader br = new BufferedReader(new FileReader(FILE));
    String line;

    int chunkSize = 100_000;
    List<String> chunkLines = new ArrayList<>(chunkSize);
    List<CompletableFuture<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
    int current=0;
    long readLineStart = System.currentTimeMillis();
    while ( (line = br.readLine()) != null) {
      if (current == chunkSize) {
        List<String> finalChunkLines = chunkLines;
        System.out.println("Total time taken for reading lines " + " took " + (System.currentTimeMillis() - readLineStart) + "ms");
        readLineStart = System.currentTimeMillis();
        futures.add(CompletableFuture.supplyAsync(() -> processLines(finalChunkLines), executorService));
        chunkLines = new ArrayList<>(chunkSize);
        current = 0;
      } else {
        chunkLines.add(line);
        current++;
      }
    }

    Map<String, MeasurementAggregator> collect =
        sequence(futures).join().stream().flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, MeasurementAggregator::merge));

    System.out.println(
        collect.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(m -> {
          MeasurementAggregator value = m.getValue();
          return m.getKey()+"="+value.min+"/"+round(value.sum/value.count)+"/"+value.max;
        }).collect(Collectors.joining(", ")));

    br.close();
  }

  private static Map<String, MeasurementAggregator> processLines(List<String> lines) {
    long start = System.currentTimeMillis();
    Map<String, MeasurementAggregator> resultMap = new HashMap<>();

    for(String line : lines) {
      String[] arr = line.split(";");
      if(arr.length != 2) {
        continue;
      }
      String city = arr[0];
      double temperature = Double.parseDouble(arr[1]);
      MeasurementAggregator measurementAggregator = resultMap.get(city);
      if(measurementAggregator == null) {
        resultMap.put(city, MeasurementAggregator.of(temperature, temperature, temperature, 1));
      } else {
        measurementAggregator.min = Math.min(measurementAggregator.min, temperature);
        measurementAggregator.max = Math.max(measurementAggregator.max, temperature);
        measurementAggregator.sum =  measurementAggregator.sum + temperature;
        measurementAggregator.count = measurementAggregator.count+1;
        resultMap.put(city, measurementAggregator);
      }
    }
    System.out.println("Total time taken for processing lines of size" + lines.size() + " took " + (System.currentTimeMillis() - start) + "ms");
    return resultMap;
  }

  private static Map<String, MeasurementAggregator> processFile(String filename) {
    long start = System.currentTimeMillis();
    Map<String, MeasurementAggregator> resultMap = new HashMap<>();
    List<CompletableFuture<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
    try (RandomAccessFile aFile = new RandomAccessFile(filename, "r");
        FileChannel inChannel = aFile.getChannel();
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();) {
        long position = 0;
        long fileSize = aFile.length();
        while(position < fileSize) {
          int end = (int) Math.min(position + CHUNK_SIZE, fileSize);
          int length = (int) (end - position);
          MappedByteBuffer mappedByteBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, position, length);
          while(mappedByteBuffer.get(length - 1) != '\n') {
            --length;
          }
          position += length;
          int finalLength = length;
          futures.add(CompletableFuture.supplyAsync(() -> processBuffer(mappedByteBuffer, finalLength), executorService).whenComplete((m, e) -> mappedByteBuffer.clear()));
        }

      executorService.shutdown();
      return
          sequence(futures).join().stream().flatMap(map -> map.entrySet().stream())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, MeasurementAggregator::merge));


    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Total time taken for processing lines took " + (System.currentTimeMillis() - start) + "ms");
    return resultMap;
  }

  private static Map<String, MeasurementAggregator> processBuffer(MappedByteBuffer mappedByteBuffer, int length) {
    Map<String, MeasurementAggregator> resultMap = new HashMap<>();
    int stationStart = 0;
    int temperatureStart = 0;
    String station = null;
    for (int i=0;i<length;i++) {
      byte b = mappedByteBuffer.get(i);
      if(b == ';') {
        byte[] stationBuff = new byte[i - stationStart];
        mappedByteBuffer.position(stationStart);
        mappedByteBuffer.get(stationBuff);
        station = new String(stationBuff, StandardCharsets.UTF_8);
        temperatureStart = i+1;
      } else if(b == '\n') {
        byte[] temperatureBuff = new byte[length - temperatureStart];
        mappedByteBuffer.position(temperatureStart);
        mappedByteBuffer.get(temperatureBuff);
        Double temperature = Double.parseDouble(new String(temperatureBuff));
        stationStart++;

        MeasurementAggregator aggregator = resultMap.computeIfAbsent(station, s -> new MeasurementAggregator());
        aggregator.min = Math.min(aggregator.min, temperature);
        aggregator.max = Math.max(aggregator.max, temperature);
        aggregator.sum += temperature;
        aggregator.count++;
      }
    }
    return resultMap;
  }

  public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    return allDoneFuture.thenApply(v ->
        futures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList())
    );
  }
}
