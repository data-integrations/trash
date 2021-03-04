/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Edwin")
@Description("Consume all input records and push to /dev/null")
public class Edwin extends ReferenceBatchSink<StructuredRecord, NullWritable, NullWritable> {
  private ThrashConfig config;

  public Edwin(ThrashConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Prepares for the run by specific the provider that encapusulates {@link NullOutputFormat}.
   * @param context of runtime for this plugin.
   */
  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(config.referenceName, new ThrashOutputFormatProvider()));

    Schema schema = context.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(context, config.referenceName, schema, "Write", "Wrote to Trash.");
    }
  }

  /**
   * Transform the {@link StructuredRecord} into the Kudu operations.
   *
   * @param input A single {@link StructuredRecord} instance
   * @param emitter for emitting records to Kudu Output format.
   */
  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, NullWritable>> emitter)
    throws Exception {
  }

  /**
   * Provider for Null Output format.
   */
  private class ThrashOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    ThrashOutputFormatProvider() throws IOException {
      this.conf = new HashMap<>();
    }

    @Override
    public String getOutputFormatClassName() {
      return NullOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  public static class ThrashConfig extends ReferencePluginConfig {

    public static final String DEFAULT_REGION = "us-central1";
    public static final String CONNECTIVITY_METHOD_IP_ALLOWLISTING = "ip-allowlisting";
    public static final String CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL = "forward-ssh-tunnel";
    public static final String AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY = "private-public-key";
    public static final String AUTHENTICATION_METHOD_PASSWORD = "password";
    public static final String DEFAULT_SID = "ORCL";

    public static final int DEFAULT_PORT = 1521;
    public static final int DEFAULT_SSH_PORT = 22;


    @Description("Whether to use an existing Datastream stream.")
    private boolean usingExistingStream;

    @Nullable
    @Description("Hostname of the Oracle server to read from. This information is required when you choose to create a " +
            "new Datastream stream.")
    private String host;

    @Nullable
    @Description("Port to use to connect to the Oracle server. This information is required when you choose to create a" +
            " new Datastream stream. By default \"" + DEFAULT_PORT + "\" will be used.")
    private Integer port;

    @Nullable
    @Description("Username to use to connect to the Oracle server. This information is required when you choose to " +
            "create a new Datastream stream.")
    private String user;

    @Nullable
    @Macro
    @Description("Password to use to connect to the Oracle server.  This information is required when you choose to " +
            "create a new Datastream stream.")
    private String password;

    @Nullable
    @Description("Oracle system identifier of the database to replicate changes from.  This information is required " +
            "when you choose to create a new Datastream stream. By default \"" + DEFAULT_SID + "\" will be used.")
    private String sid;

    @Nullable
    @Description(
            "Region of the existing Datastream stream or a new stream to be created. By default \"" + DEFAULT_REGION +
                    "\" will be used.")
    private String region;

    @Nullable
    @Description("The way DataStream will connect to Oracle. See \"Documentation\" tab for details.")
    private String connectivityMethod;

    @Nullable
    @Description("Hostname of the SSH Server to connect to.")
    // only required when connectivity method is  "Forward SSH Tunnel"
    private String sshHost;

    @Nullable
    @Description("Port of the SSH Server to connect to. By default \"" + DEFAULT_SSH_PORT + "\" will be used.")
    // Cannot make sshPort an int, because UI will take this property as required and thus cannot hide
    // this property when IP allowlisting is selected as connectivity method
    // only required when connectivity method is  "Forward SSH Tunnel"
    private Integer sshPort;

    @Nullable
    @Description("Username to login the SSH Server.")
    // only required when connectivity method is  "Forward SSH Tunnel"
    private String sshUser;

    @Nullable
    @Description("How the SSH server authenticates the login.")
    // only required when connectivity method is  "Forward SSH Tunnel"
    private String sshAuthenticationMethod;

    @Macro
    @Nullable
    @Description("Password of the login on the SSH Server.")
    // only required when connectivity method is  "Forward SSH Tunnel" and authentication method is
    // "Password"
    private String sshPassword;

    @Macro
    @Nullable
    @Description("Private key of the login on the SSH Server.")
    // only required when connectivity method is  "Forward SSH Tunnel" and authentication method is
    // "Private/Public Key Pair"
    private String sshPrivateKey;

    @Nullable
    @Description("The GCS bucket that DataStream can write its output to. By default replicator " +
            "application will create one for you. See \"Documentation\" tab for details")
    private String gcsBucket;

    @Nullable
    @Description("The optional path prefix of the path where DataStream will write its output to.")
    private String gcsPathPrefix;


    @Macro
    @Nullable
    @Description("The service account key of the service account that will be used to read " +
            "DataStream results from GCS Bucket. By default Dataproc service account will be used.")
    private String gcsServiceAccountKey;

    @Nullable
    @Description("The id of an existing Datastream stream that will be used to read CDC changes from.  This information" +
            " is required when you choose to use an existing Datastream stream.")
    private String streamId;

    @Macro
    @Nullable
    @Description("The service account key of the service account that will be used to create or query DataStream stream" +
            ". By default Cloud Data Fusion service account will be used when you create a replicator and Dataproc service " +
            "account will be used when replicator pipeline is running.")
    private String dsServiceAccountKey;

    @Nullable
    @Description("Project of the Datastream stream. When running on Google Cloud Platform, this can be set to "
            + "'auto-detect', which will use the project of the Google Cloud Data Fusion.")
    private String project;

    public ThrashConfig(boolean usingExistingStream, @Nullable String host, @Nullable Integer port,
                            @Nullable String user, @Nullable String password, @Nullable String sid, @Nullable String region,
                            @Nullable String connectivityMethod, @Nullable String sshHost, @Nullable Integer sshPort, @Nullable String sshUser,
                            @Nullable String sshAuthenticationMethod, @Nullable String sshPassword, @Nullable String sshPrivateKey,
                            @Nullable String gcsBucket, @Nullable String gcsPathPrefix, @Nullable String gcsServiceAccountKey,
                            @Nullable String dsServiceAccountKey, @Nullable String streamId, @Nullable String project) {
      super("test");
      this.usingExistingStream = usingExistingStream;
      this.host = host;
      this.port = port;
      this.user = user;
      this.password = password;
      this.sid = sid;
      this.region = region;
      this.connectivityMethod = connectivityMethod;
      this.sshHost = sshHost;
      this.sshPort = sshPort;
      this.sshUser = sshUser;
      this.sshAuthenticationMethod = sshAuthenticationMethod;
      this.sshPassword = sshPassword;
      this.sshPrivateKey = sshPrivateKey;
      this.gcsBucket = gcsBucket;
      this.gcsPathPrefix = gcsPathPrefix;
      this.gcsServiceAccountKey = gcsServiceAccountKey;
      this.dsServiceAccountKey = dsServiceAccountKey;
      this.streamId = streamId;
      this.project = project;
    }
  }
}
