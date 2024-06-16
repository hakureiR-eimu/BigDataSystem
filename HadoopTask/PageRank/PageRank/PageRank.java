// 导入 Java 和 Hadoop 相关的库

import static java.lang.Math.abs;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
    public PageRank() {}

    public static enum PageCounter { Converge, Total }

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
        String inpath = otherArgs[ 0 ];
        String outpath = otherArgs[ 1 ] + "/iter";
        for ( int i = 0; i < 50; i++ ) {
            // 设置 Hadoop Job
            Job job = Job.getInstance( conf, "" );
            job.setJarByClass( PageRank.class );
            job.setMapperClass( PageRankMapper.class );
            job.setReducerClass( PageRankReducer.class );
            job.setOutputKeyClass( Text.class );
            job.setOutputValueClass( Text.class );
            // 添加输入文件
            FileInputFormat.addInputPath( job, new Path( inpath ) );
            // 设置输出文件
            FileOutputFormat.setOutputPath( job, new Path( outpath + i ) );
            // 下轮输入文件
            inpath = outpath + i;
            job.waitForCompletion( true );

            Counters counters = job.getCounters();
            long totalpage = counters.findCounter( PageCounter.Total )
                                 .getValue(); // 获取计数值
            long convergepage = counters.findCounter( PageCounter.Converge )
                                    .getValue(); //收敛计数值
            System.out.println( "total page: " + totalpage );
            System.out.println( "converge page: " + convergepage );
            if ( (double) convergepage / totalpage >= 0.99 ) {
                System.out.println( "converge at iteration: " + i );
                break;
            }
        }
    }

    public static class PageRankReducer
        extends Reducer<Text, Text, Text, Text> {

        public PageRankReducer() {}

        public void reduce( Text key, Iterable<Text> values,
                            Reducer<Text, Text, Text, Text>.Context context )
            throws IOException, InterruptedException {
            StringBuffer links = new StringBuffer();
            double newRank = 0.0, preRank = 0.0;

            for ( Text value : values ) {

                String content = value.toString();

                if ( content.startsWith( "|" ) ) {
                    // if this value contains node links append them to the
                    // 'links' string for future use: this is needed to
                    // reconstruct the Formatinput for Job#2 mapper in case of
                    // multiple iterations of it.
                    links.append( content.substring( 1 ) );
                } else if ( content.startsWith( "#" ) ) {

                    double in_rank =
                        Double.parseDouble( content.trim().substring( 1 ) );

                    newRank += in_rank;
                } else if ( content.startsWith( "&" ) ) {
                    preRank =
                        Double.parseDouble( content.trim().substring( 1 ) );
                }
            }

            newRank = 0.85 * newRank + 0.15 * 3.547319468043973e-6;
            context.write( key, new Text( newRank + " " + links ) );

            if ( abs( newRank - preRank ) <= 1E-8 )
                context.getCounter( PageCounter.Converge ).increment( 1 );
        }
    }

    public static class PageRankMapper
        extends Mapper<Object, Text, Text, Text> {

        public PageRankMapper() {}

        public void map( Object key, Text value,
                         Mapper<Object, Text, Text, Text>.Context context )
            throws IOException, InterruptedException {
            int idx1 = value.find( "\t" );
            int idx2 = value.find( " " );

            String page = Text.decode( value.getBytes(), 0, idx1 );
            String rank =
                Text.decode( value.getBytes(), idx1 + 1, idx2 - idx1 - 1 );
            String links = Text.decode( value.getBytes(), idx2 + 1,
                                        value.getLength() - ( idx2 + 1 ) );

            String[] allOtherPages = links.split( " " );
            int outs = allOtherPages.length;
            for ( String otherPage : allOtherPages ) {
                if ( Objects.equals( otherPage, "" ) ) continue;
                StringBuffer out = new StringBuffer( "#" );
                out.append( Double.parseDouble( rank ) / outs );
                Text outPageRank = new Text( out.toString() );
                context.write( new Text( otherPage ), outPageRank );
            }

            context.write( new Text( page ), new Text( "&" + rank ) );
            // put the original links so the reducer is able to produce the
            // correct output
            context.write( new Text( page ), new Text( "|" + links ) );

            context.getCounter( PageCounter.Total ).increment( 1 );
        }
    }
}