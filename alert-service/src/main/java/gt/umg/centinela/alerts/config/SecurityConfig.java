package gt.umg.centinela.alerts.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Control de acceso (ADR-007): solo operadores autenticados ven alertas y solo administradores
 * modifican la lista de placas robadas. Las metricas para Prometheus quedan abiertas dentro de la
 * red de servicios (en produccion se restringen por firewall/segmento de red).
 */
@Configuration
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    public UserDetailsService users(AlertServiceProperties properties, PasswordEncoder encoder) {
        AlertServiceProperties.Security s = properties.security();
        return new InMemoryUserDetailsManager(
                User.withUsername(s.operatorUser()).password(encoder.encode(s.operatorPassword())).roles("OPERATOR").build(),
                User.withUsername(s.adminUser()).password(encoder.encode(s.adminPassword())).roles("OPERATOR", "ADMIN").build());
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll()
                        .requestMatchers("/api/admin/**").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.POST, "/api/watchlist/**").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.DELETE, "/api/watchlist/**").hasRole("ADMIN")
                        .requestMatchers("/api/**").hasRole("OPERATOR")
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults());
        return http.build();
    }
}
