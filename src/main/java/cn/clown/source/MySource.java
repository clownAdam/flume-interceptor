package cn.clown.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * @author clown
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
    /**
     * 定义全局的前缀和后缀
     * prefix没有默认值
     * suffix有默认值:clown
     */
    private String prefix;
    private String suffix;
    private final int FOR_SIZE = 5;
    /**
     * @param context
     */
    @Override
    public void configure(Context context) {
        //读取配置信息,给前后缀赋值
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "clown");
    }

    /**
     * 1.接收数据(for循环造数据)
     * 2.封装为事件
     * 3.将事件传给channel
     *
     * @return 状态
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        //0.定义当前状态
        Status status = null;
        try {
            //1.接收数据
            for (int i = 0; i < FOR_SIZE; i++) {
                //2.构建事件对象
                Event event = new SimpleEvent();
                //3.给事件设置值.prefix和suffix需要从外部传入;prefix没有默认值,suffix有默认值
                event.setBody((prefix + "--" + i + "--" + suffix).getBytes());
                //4.将事件传给channel
                getChannelProcessor().processEvent(event);
            }
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            //backoff:退避算法
            status = Status.BACKOFF;
        }
        //休眠2s
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
