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

package co.cask;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Trash")
@Description("Consume all input records and push to /dev/null")
public class Trash extends ReferenceBatchSink<StructuredRecord, NullWritable, NullWritable> {
  private ThrashConfig config;

  public Trash(ThrashConfig config) {
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
    public ThrashConfig(String referenceName) {
      super(referenceName);
    }
  }
}
