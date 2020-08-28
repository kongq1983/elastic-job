package com.kq.elasticjob.simple;

import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * @author kq
 * @date 2020-08-27 10:17
 * @since 2020-0630
 */
public class MyElasticJob implements SimpleJob {

    int timeout = 1;

    @Override
    public void execute(ShardingContext shardingContext) {
        switch (shardingContext.getShardingItem()) {
            case 0: {
                System.out.println(LocalDateTime.now()+",当前分片：" + shardingContext.getShardingItem() + "=====" + "参数："
                        + shardingContext.getShardingParameter() + " =====" + Thread.currentThread());

                try {
                    TimeUnit.MINUTES.sleep(timeout);
                }catch (Exception e){
                    e.printStackTrace();
                }

                System.out.println("=============================================================1");

                break;
            }
            case 1: {
                System.out.println(LocalDateTime.now()+",当前分片：" + shardingContext.getShardingItem() + "=====" + "参数："
                        + shardingContext.getShardingParameter() + " =====" + Thread.currentThread());

                try {
                    TimeUnit.MINUTES.sleep(timeout);
                }catch (Exception e){
                    e.printStackTrace();
                }
                System.out.println("=============================================================2");
                break;
            }
            case 2: {
                System.out.println(LocalDateTime.now()+",当前分片：" + shardingContext.getShardingItem() + "=====" + "参数："
                        + shardingContext.getShardingParameter() + " =====" + Thread.currentThread());

                try {
                    TimeUnit.MINUTES.sleep(timeout);
                }catch (Exception e){
                    e.printStackTrace();
                }
                System.out.println("=============================================================3");
                break;
            }
            default: {
                System.out.println(LocalDateTime.now()+",当前分片：" + shardingContext.getShardingItem() + "=====" + "参数："
                        + shardingContext.getShardingParameter() + " =====" + Thread.currentThread());
                break;
            }
        }
    }
}
