package company.vk.edu.distrib.compute;

import java.util.Set;
import java.util.stream.Stream;

import company.vk.edu.distrib.compute.arseniy90.ArzhKVServiceFactoryImpl;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.InMemoryKVServiceFactory;
import company.vk.edu.distrib.compute.b10nicle.B10nicleKVServiceFactory;
import company.vk.edu.distrib.compute.nihuaway00.NihuawayKVServiceFactory;
import company.vk.edu.distrib.compute.mandesero.KVServiceFactoryImpl;
import company.vk.edu.distrib.compute.vitos23.Vitos23KVServiceFactory;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.junit.platform.commons.util.ReflectionUtils;

public class KVServiceFactoryArgumentsProvider implements ArgumentsProvider {
    private final Set<Class<? extends KVServiceFactory>> factories = Set.of(
        ArzhKVServiceFactoryImpl.class,
        KVServiceFactoryImpl.class,
        InMemoryKVServiceFactory.class,
        Vitos23KVServiceFactory.class,
        NihuawayKVServiceFactory.class,
        B10nicleKVServiceFactory.class
    );

    @Override
    @NonNull
    public Stream<? extends Arguments> provideArguments(
        @NonNull ParameterDeclarations parameters,
        @NonNull ExtensionContext context
    ) {
        return factories.stream()
            .map(ReflectionUtils::newInstance)
            .map(Arguments::of);
    }
}
