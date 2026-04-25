package company.vk.edu.distrib.compute;

import java.util.Set;
import java.util.stream.Stream;

import company.vk.edu.distrib.compute.arseniy90.ArzhKVServiceFactoryImpl;
// import company.vk.edu.distrib.compute.aldor7705.KVServiceFactorySimple;
// import company.vk.edu.distrib.compute.andeco.AndecoKVServiceFactory;
// import company.vk.edu.distrib.compute.artttnik.MyKVServiceFactory;
// import company.vk.edu.distrib.compute.b10nicle.B10nicleKVServiceFactory;
// import company.vk.edu.distrib.compute.gavrilova_ekaterina.InMemoryKVServiceFactory;
// import company.vk.edu.distrib.compute.ip.PopovIgorKVServiceFactoryImpl;
// import company.vk.edu.distrib.compute.kirillmedvedev23.KirillmedvedevFileSystemKVServiceFactory;
// import company.vk.edu.distrib.compute.kirillmedvedev23.KirillmedvedevKVServiceFactory;
// import company.vk.edu.distrib.compute.korjick.CakeKVServiceFactory;
// import company.vk.edu.distrib.compute.luckyslon2003.LuckySlon2003KVServiceFactory;
// import company.vk.edu.distrib.compute.mandesero.KVServiceFactoryImpl;
// import company.vk.edu.distrib.compute.nesterukia.file_system.NesterukiaFileSystemKVServiceFactory;
// import company.vk.edu.distrib.compute.nihuaway00.NihuawayKVServiceFactory;
// import company.vk.edu.distrib.compute.shuuuurik.ShuuuurikFileKVServiceFactory;
// import company.vk.edu.distrib.compute.vitos23.Vitos23KVServiceFactory;
// import company.vk.edu.distrib.compute.vredakon.VredakonKVServiceFactory;
// import company.vk.edu.distrib.compute.wolfram158.Wolfram158KVServiceFactoryFileWithCacheImpl;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.junit.platform.commons.util.ReflectionUtils;

public class KVServiceFactoryArgumentsProvider implements ArgumentsProvider {

    private final Set<Class<? extends KVServiceFactory>> factories = Set.of(
        ArzhKVServiceFactoryImpl.class
    // KVServiceFactorySimple.class,
    // KVServiceFactoryImpl.class,
    // AndecoKVServiceFactory.class,
    // MyKVServiceFactory.class,
    // InMemoryKVServiceFactory.class,
    // Vitos23KVServiceFactory.class,
    // NihuawayKVServiceFactory.class,
    // ShuuuurikFileKVServiceFactory.class,
    // B10nicleKVServiceFactory.class,
    // VredakonKVServiceFactory.class,
    // PopovIgorKVServiceFactoryImpl.class,
    // NesterukiaFileSystemKVServiceFactory.class,
    // KirillmedvedevKVServiceFactory.class,
    // KirillmedvedevFileSystemKVServiceFactory.class,
    // LuckySlon2003KVServiceFactory.class,
    // Wolfram158KVServiceFactoryFileWithCacheImpl.class,
    // CakeKVServiceFactory.class
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
