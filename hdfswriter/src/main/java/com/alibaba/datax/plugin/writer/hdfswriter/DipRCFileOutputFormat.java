package com.alibaba.datax.plugin.writer.hdfswriter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jiaomingbei on 2017/7/19.
 */
public class DipRCFileOutputFormat extends FileOutputFormat<WritableComparable, BytesRefArrayWritable> implements HiveOutputFormat<WritableComparable, BytesRefArrayWritable> {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsWriter.Job.class);

    public DipRCFileOutputFormat() {
    }

    public static void setColumnNumber(Configuration conf, int columnNum) {
        assert columnNum > 0;

        conf.setInt(HiveConf.ConfVars.HIVE_RCFILE_COLUMN_NUMBER_CONF.varname, columnNum);
    }

    public static int getColumnNumber(Configuration conf) {
        return HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_RCFILE_COLUMN_NUMBER_CONF);
    }

    public RecordWriter<WritableComparable, BytesRefArrayWritable> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        Path outputPath = new Path(name);
        FileSystem fs = outputPath.getFileSystem(job);
        Path file = new Path(outputPath, name);
        CompressionCodec codec = null;
        if(getCompressOutput(job)) {
            Class<?> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
        }

        final RCFile.Writer out = new RCFile.Writer(fs, job, file, progress, codec);

        return new RecordWriter<WritableComparable, BytesRefArrayWritable>() {
            public void close(Reporter reporter) throws IOException {
                out.close();
            }

            public void write(WritableComparable key, BytesRefArrayWritable value) throws IOException {
                out.append(value);
            }
        };
    }

    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {
        String[] cols = null;
        String columns = tableProperties.getProperty("columns");
        if(columns != null && !columns.trim().equals("")) {
            cols = StringUtils.split(columns, ",");
        } else {
            cols = new String[0];
        }

        setColumnNumber(jc, cols.length);
        final RCFile.Writer outWriter = Utilities.createRCFileWriter(jc, finalOutPath.getFileSystem(jc), finalOutPath, isCompressed, progress);
        return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {
            public void write(Writable r) throws IOException {
                outWriter.append(r);
            }

            public void close(boolean abort) throws IOException {
                outWriter.close();
            }
        };
    }
}

