package com.geek.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalup = 0;
        long totaldown = 0;
        for (FlowBean value : values) {
            totalup += value.getUpFlow();
            totaldown += value.getDownFlow();
        }
        outV.setUpFlow(totalup);
        outV.setDownFlow(totaldown);
        outV.setSumFlow();

        context.write(key, outV);
    }

}
