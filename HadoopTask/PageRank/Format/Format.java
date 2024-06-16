// 导入 Java 和 Hadoop 相关的库

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import javax.xml.soap.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Format {
    public static Set<String> NODES = new HashSet<String>();

    public Format() {}

    public static void main( String[] args ) throws Exception {
        // 设置 Hadoop Configuration
        Configuration conf = new Configuration();

        // 使用 GenericOptionsParser 获取命令行参数
        String[] otherArgs =
            ( new GenericOptionsParser( conf, args ) ).getRemainingArgs();

        // 如果输入参数个数 <2 则返回错误提示
        if ( otherArgs.length < 2 ) {
            System.err.println( "Usage: wordcount <in> [<in>...] <out>" );
            System.exit( 2 );
        }

        // 设置 Hadoop Job
        Job job = Job.getInstance( conf, "" );
        job.setJarByClass( Format.class );
        job.setMapperClass( FormatMapper.class );
        job.setCombinerClass( FormatCombiner.class );
        job.setReducerClass( FormatReducer.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        // 添加输入文件
        for ( int i = 0; i < otherArgs.length - 1; ++i ) {
            FileInputFormat.addInputPath( job, new Path( otherArgs[ i ] ) );
        }

        // 设置输出文件
        FileOutputFormat.setOutputPath(
            job, new Path( otherArgs[ otherArgs.length - 1 ] ) );

        // 提交任务并等待任务完成，如果成功则返回0，反之则返回1
        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }

    public static class FormatReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public FormatReducer() {}

        public void reduce( Text key, Iterable<Text> values,
                            Reducer<Text, Text, Text, Text>.Context context )
            throws IOException, InterruptedException {
            StringBuilder links = new StringBuilder();

            double rank = 1.0 / NODES.size();
            links.append( rank );
            for ( Text value : values ) {
                links.append( " " ).append( value.toString() );
            }

            // 写入结果
            this.result.set( links.toString().trim() );
            context.write( key, this.result );
        }
    }

    public static class FormatCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public FormatCombiner() {}

        public void reduce( Text key, Iterable<Text> values,
                            Reducer<Text, Text, Text, Text>.Context context )
            throws IOException, InterruptedException {
            StringBuilder links = new StringBuilder();

            for ( Text value : values ) {
                links.append( " " ).append( value.toString() );
            }

            // 写入结果
            this.result.set( links.toString().trim() );
            context.write( key, this.result );
        }
    }

    public static class FormatMapper extends Mapper<Object, Text, Text, Text> {
        private Text nodaA = new Text();
        private Text nodaB = new Text();

        public FormatMapper() {}

        public void map( Object key, Text value,
                         Mapper<Object, Text, Text, Text>.Context context )
            throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer( value.toString() );

            this.nodaA.set( itr.nextToken() );
            NODES.add( this.nodaA.toString() );
            this.nodaB.set( itr.nextToken() );
            NODES.add( this.nodaB.toString() );

            context.write( this.nodaA, this.nodaB );
        }
    }
}
