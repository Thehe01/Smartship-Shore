package com.smartship.shore.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * P2-3 Redis wiring. The connection itself comes from Spring Boot's Lettuce
 * auto-configuration ({@code spring.data.redis.host/port}); only the compare-and-set
 * Lua script needs an explicit bean here.
 */
@Configuration
public class RedisConfig {

  /**
   * Atomic latest-state compare-and-set script ({@code UPDATED} / {@code STALE}).
   * Loaded once from the classpath; executed with key + timestampARGS per write.
   */
  @Bean
  public DefaultRedisScript<String> latestStateCasScript() {
    DefaultRedisScript<String> script = new DefaultRedisScript<>();
    script.setLocation(new ClassPathResource("redis/latest_state_cas.lua"));
    script.setResultType(String.class);
    return script;
  }
}
