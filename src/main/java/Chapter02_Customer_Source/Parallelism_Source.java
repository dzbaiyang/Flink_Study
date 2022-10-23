package Chapter02_Customer_Source;

import Chapter02_Customer_Source.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class Parallelism_Source implements ParallelSourceFunction<Event> {
    //声明标识位
    private boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        //随机生成器
        Random random = new Random();
        //定义字段选取的数据集
        String[] users = {"Ling","Bob","FakeBob"};
        String[] urls = {"./home","./cart","./fav","./prod?id=100","./prod?id=200"};


        //循环生成数据
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}