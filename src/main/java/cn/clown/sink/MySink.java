package cn.clown.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author clown
 */
public class MySink extends AbstractSink implements Configurable {
    /**
     * 定义logger对象
     */
    private Logger logger = null;
    /**
     * 定义前后缀
     */
    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        logger = LoggerFactory.getLogger(MySink.class);
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "clown");
    }

    /**
     * 1.获取channel
     * 2.从channel获取事务以及数据
     * 3.发送数据
     *
     * @return 状态
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        //0.定义返回值
        Status status = null;
        //1.获取channel
        Channel channel = getChannel();
        //2.从channel中获取事务
        Transaction transaction = channel.getTransaction();
        //3.开启事务
        transaction.begin();
        try {
            //4.从channel中获取数据
            Event event = channel.take();
            //TODO:5.处理事件
            if (event != null) {
                //获取body
                String body = new String(event.getBody());
                //logger
                logger.info(body);
            } else {
                //修改状态
                status = Status.BACKOFF;
            }
            //6.提交事务
            transaction.commit();
            //成功提交,修改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            //提交事务失败
            transaction.rollback();
        } finally {
            //关闭事务
            transaction.close();
        }

        return status;
    }
}
