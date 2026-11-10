package org.aiopslab.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpNormalizerTest {
    @Test
    void keepsDeclaredServiceNameAsAuthoritative() {
        assertEquals("checkout", OtlpNormalizer.canonicalServiceName(Map.of(
                "service.name", "checkout",
                "k8s.deployment.name", "checkout-deployment")));
    }

    @Test
    void fallsBackOnlyToAWorkloadController() {
        assertEquals("checkout", OtlpNormalizer.canonicalServiceName(Map.of(
                "service.name", "unknown",
                "k8s.deployment.name", "checkout")));
        assertEquals("cart", OtlpNormalizer.canonicalServiceName(Map.of(
                "k8s.statefulset.name", "cart")));
    }

    @Test
    void doesNotInventAServiceFromPodOrContainerNames() {
        assertEquals("unknown", OtlpNormalizer.canonicalServiceName(Map.of(
                "k8s.pod.name", "checkout-abc-123",
                "k8s.container.name", "checkout")));
    }
}
