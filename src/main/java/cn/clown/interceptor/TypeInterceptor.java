package cn.clown.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author clown
 */
public class TypeInterceptor implements Interceptor {

    /**
     * 声明一个存放事件的集合
     */
    private List<Event> addHeaderEvents;

    /**
     * 初始化
     */
    @Override
    public void initialize() {
        //初始化
        addHeaderEvents = new ArrayList<>();
    }

    /**
     * 对单个事件进行拦截处理
     *
     * @param event
     * @return 返回事件
     */
    @Override
    public Event intercept(Event event) {
        //1.获取事件中的header头信息
        Map<String, String> headers = event.getHeaders();
        //2.获取事件中的body信息
        String body = new String(event.getBody());
        //3.根据body中是否有"hello"来决定添加怎样的头信息
        String hello = "hello";
        if (body.contains(hello)) {
            headers.put("type", "hello");
        } else {
            headers.put("type", "noHello");
        }
        return event;
    }

    /**
     * 对多个event事件进行拦截处理
     *
     * @param events 多个事件
     * @return 返回多个事件
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        //1.清空集合
        addHeaderEvents.clear();
        //2.遍历events
        for (Event event : events) {
            //3.给每个事件添加头信息
            addHeaderEvents.add(intercept(event));
        }
        return addHeaderEvents;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
