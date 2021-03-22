/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.service;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.common.models.FeatureV2;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetInferenceResponse;
import feast.proto.serving.ServingAPIProto.OnlineModelRunRequest;
import feast.proto.serving.ServingAPIProto.Pong;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.Feature;
import feast.storage.connectors.redis.common.RedisHashDecoder;
import feast.storage.connectors.redis.common.RedisKeyGenerator;
import feast.storage.connectors.redis.retriever.RedisClientAdapter;
import io.grpc.Status;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.ByteArrayOutput;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.KeyValueListOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OnlineStorePlugins implements OnlineServicePluginsV1 {

  private enum RedisAIProtocolKeyword implements ProtocolKeyword {
    DAGRUN;

    private final byte name[];

    RedisAIProtocolKeyword() {
      // cache the bytes for the command name. Reduces memory and cpu pressure when using commands.
      name = ("AI." + name()).getBytes();
    }

    @Override
    public byte[] getBytes() {
      return name;
    }
  }

  private final RedisClientAdapter redisClientAdapter;
  private final Tracer tracer;
  private final CachedSpecService specService;
  private static final String timestampPrefix = "_ts";

  private final CommandOutput<byte[], byte[], byte[]> pingOutput =
      new ByteArrayOutput<>(new ByteArrayCodec());

  private final CommandOutput<byte[], byte[], List<KeyValue<byte[], byte[]>>> redisaioutput =
      new KeyValueListOutput<byte[], byte[]>(new ByteArrayCodec());

  public OnlineStorePlugins(
      RedisClientAdapter redisClientAdapter, Tracer tracer, CachedSpecService specService) {
    this.redisClientAdapter = redisClientAdapter;
    this.tracer = tracer;
    this.specService = specService;
  }

  @Override
  public Pong ping() {

    RedisFuture<byte[]> f = redisClientAdapter.dispatch(CommandType.PING, pingOutput);
    redisClientAdapter.flushCommands();
    String s = null;
    try {
      s = new String(f.get(), StandardCharsets.UTF_8);
    } catch (InterruptedException | ExecutionException e) {
      throw Status.UNKNOWN
          .withDescription("Unexpected error when pulling data from from Redis.")
          .withCause(e)
          .asRuntimeException();
    }
    return Pong.newBuilder().setVal(s).build();
  }

  private static Map<FeatureReferenceV2, Feature> getFeatureRefFeatureMap(List<Feature> features) {
    return features.stream()
        .collect(Collectors.toMap(Feature::getFeatureReference, Function.identity()));
  }

  private List<List<Feature>> runInference(
      String model_name,
      List<RedisProto.RedisKeyV2> redisKeys,
      List<FeatureReferenceV2> featureReferences,
      List<String> outputs) {
    List<List<Feature>> features = new ArrayList<>();
    // To decode bytes back to Feature Reference
    Map<String, ServingAPIProto.FeatureReferenceV2> byteToFeatureReferenceMap = new HashMap<>();

    // Serialize using proto
    List<byte[]> binaryRedisKeys =
        redisKeys.stream().map(redisKey -> redisKey.toByteArray()).collect(Collectors.toList());

    List<byte[]> featureReferenceByteList = new ArrayList<>();
    featureReferences.stream()
        .forEach(
            featureReference -> {

              // eg. murmur(<featuretable_name:feature_name>)
              byte[] featureReferenceBytes =
                  RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReference);
              featureReferenceByteList.add(featureReferenceBytes);
              byteToFeatureReferenceMap.put(featureReferenceBytes.toString(), featureReference);

              // // eg. <_ts:featuretable_name>
              // byte[] featureTableTsBytes =
              //     RedisHashDecoder.getTimestampRedisHashKeyBytes(featureReference,
              // timestampPrefix);
              // featureReferenceWithTsByteList.add(featureTableTsBytes);
            });

    // Perform a series of independent calls
    List<RedisFuture<List<KeyValue<byte[], byte[]>>>> futures = Lists.newArrayList();
    for (byte[] binaryRedisKey : binaryRedisKeys) {
      byte[][] featureReferenceByteArrays = featureReferenceByteList.toArray(new byte[0][]);
      // Access redis keys and extract features
      CommandArgs<byte[], byte[]> args = new CommandArgs<byte[], byte[]>(new ByteArrayCodec());
      args.add("|>".getBytes());
      args.add("from_feast".getBytes());
      args.add(binaryRedisKey);
      for (int i = 0; i < featureReferenceByteArrays.length; i++) {
        args.add(featureReferenceByteArrays[i]);
      }
      args.add("|>".getBytes());
      args.add("AI.MODELRUN".getBytes());
      args.add(model_name.getBytes());
      args.add("INPUTS".getBytes());
      for (int i = 0; i < featureReferenceByteArrays.length; i++) {
        args.add(featureReferenceByteArrays[i]);
      }
      args.add("OUTPUTS".getBytes());
      for (int i = 0; i < outputs.size(); i++) {
        args.add(outputs.get(i).getBytes());
      }
      args.add("|>".getBytes());
      args.add("to_feast".getBytes());
      for (int i = 0; i < outputs.size(); i++) {
        args.add(outputs.get(i).getBytes());
      }

      futures.add(redisClientAdapter.dispatch(RedisAIProtocolKeyword.DAGRUN, redisaioutput, args));
    }

    // Write all commands to the transport layer
    redisClientAdapter.flushCommands();

    futures.forEach(
        future -> {
          try {
            List<KeyValue<byte[], byte[]>> redisValuesList = future.get();
            List<Feature> curRedisKeyFeatures =
                RedisHashDecoder.retrieveFeature(
                    redisValuesList, byteToFeatureReferenceMap, timestampPrefix);
            features.add(curRedisKeyFeatures);
          } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
            throw Status.UNKNOWN
                .withDescription("Unexpected error when pulling data from from Redis.")
                .withCause(e)
                .asRuntimeException();
          }
        });
    return features;
  }

  private List<List<Feature>> getInferenceResults(
      String model_name,
      String project,
      List<OnlineModelRunRequest.EntityRow> entityRows,
      List<FeatureReferenceV2> featureReferences,
      List<String> outputs) {

    List<RedisProto.RedisKeyV2> redisKeys =
        RedisKeyGenerator.buildInferenceRedisKeys(project, entityRows);
    return runInference(model_name, redisKeys, featureReferences, outputs);
  }

  @Override
  public GetInferenceResponse modelRun(OnlineModelRunRequest request) {
    String projectName = request.getProject();
    String modelName = request.getModelName();
    List<String> outputs = request.getOutputsList();
    List<FeatureReferenceV2> featureReferences = request.getFeaturesList();

    // Autofill default project if project is not specified
    if (projectName.isEmpty()) {
      projectName = "default";
    }

    List<OnlineModelRunRequest.EntityRow> entityRows = request.getEntityRowsList();
    List<Map<String, ValueProto.Value>> values =
        entityRows.stream().map(r -> new HashMap<>(r.getFieldsMap())).collect(Collectors.toList());

    Span inferenceSpan = tracer.buildSpan("inferenceSpan").start();
    if (inferenceSpan != null) {
      inferenceSpan.setTag("entities", entityRows.size());
      inferenceSpan.setTag("features", featureReferences.size());
    }
    List<List<Feature>> entityRowsFeatures =
        getInferenceResults(modelName, projectName, entityRows, featureReferences, outputs);
    if (inferenceSpan != null) {
      inferenceSpan.finish();
    }

    if (entityRowsFeatures.size() != entityRows.size()) {
      throw Status.INTERNAL
          .withDescription(
              "The no. of FeatureRow obtained from OnlineRetriever"
                  + "does not match no. of entityRow passed.")
          .asRuntimeException();
    }

    String finalProjectName = projectName;
    Map<FeatureReferenceV2, ValueProto.ValueType.Enum> featureValueTypes =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> {
                      try {
                        return specService.getFeatureSpec(finalProjectName, ref).getValueType();
                      } catch (SpecRetrievalException e) {
                        return ValueProto.ValueType.Enum.INVALID;
                      }
                    }));

    Span postProcessingSpan = tracer.buildSpan("postProcessing").start();

    for (int i = 0; i < entityRows.size(); i++) {
      OnlineModelRunRequest.EntityRow entityRow = entityRows.get(i);
      List<Feature> curEntityRowFeatures = entityRowsFeatures.get(i);

      Map<FeatureReferenceV2, Feature> featureReferenceFeatureMap =
          getFeatureRefFeatureMap(curEntityRowFeatures);

      Map<String, ValueProto.Value> rowValues = values.get(i);

      for (FeatureReferenceV2 featureReference : featureReferences) {
        Map<String, ValueProto.Value> valueMap =
            new HashMap<>() {
              {
                put(
                    FeatureV2.getFeatureStringRef(featureReference),
                    ValueProto.Value.newBuilder().build());
              }
            };
        rowValues.putAll(valueMap);
      }
    }

    if (postProcessingSpan != null) {
      postProcessingSpan.finish();
    }

    // populateHistogramMetrics(entityRows, featureReferences, projectName);
    // populateFeatureCountMetrics(featureReferences, projectName);

    // Response field values should be in the same order as the entityRows provided by the user.
    List<GetInferenceResponse.InfernceResponse> infernceResponseList =
        IntStream.range(0, entityRows.size())
            .mapToObj(
                entityRowIdx ->
                    GetInferenceResponse.InfernceResponse.newBuilder()
                        .putAllValues(values.get(entityRowIdx))
                        .build())
            .collect(Collectors.toList());
    return GetInferenceResponse.newBuilder().addAllInferenceResponse(infernceResponseList).build();
  }
}
