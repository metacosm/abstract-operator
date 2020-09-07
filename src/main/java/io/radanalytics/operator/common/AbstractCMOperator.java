package io.radanalytics.operator.common;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.radanalytics.operator.resource.LabelsHelper;

/**
 * @author <a href="claprun@redhat.com">Christophe Laprun</a>
 */
public abstract class AbstractCMOperator<T extends EntityInfo> extends AbstractOperator<T> {
    private Map<String, String> selector;
    
    
    @Override
    protected CompletableFuture<? extends AbstractWatcher<T>> initializeWatcher() {
        ConfigMapWatcher.Builder<T> cmBuilder = new ConfigMapWatcher.Builder<>();
        ConfigMapWatcher cmWatcher = cmBuilder.withClient(client)
            .withSelector(selector)
            .withEntityName(entityName)
            .withNamespace(namespace)
            .withConvert(this::convert)
            .withOnAdd(this::onAdd)
            .withOnDelete(this::onDelete)
            .withOnModify(this::onModify)
            .withPredicate(this::isSupported)
            .build();
        return cmWatcher.watch();
    }
    
    @Override
    protected void onInit() {
        super.onInit();
        this.selector = LabelsHelper.forKind(entityName, prefix);
    }
    
    /**
     * Implicitly only those configmaps with given prefix and kind are being watched, but you can provide additional
     * 'deep' checking in here.
     *
     * @param cm ConfigMap that is about to be checked
     * @return true if cm is the configmap we are interested in
     */
    protected boolean isSupported(ConfigMap cm) {
        return true;
    }
    
    /**
     * Converts the configmap representation into T.
     * Normally, you may want to call something like:
     *
     * <code>HasDataHelper.parseCM(FooBar.class, cm);</code> in this method, where FooBar is of type T.
     * This would parse the yaml representation of the configmap's config section and creates an object of type T.
     *
     * @param cm ConfigMap that is about to be converted to T
     * @return entity of type T
     */
    protected T convert(ConfigMap cm) {
        return ConfigMapWatcher.defaultConvert(infoClass, cm);
    }
    
    @Override
    protected Set<T> getDesiredSet() {
        MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> aux1 =
            client.configMaps();
        FilterWatchListMultiDeletable<ConfigMap, ConfigMapList, Boolean, Watch, Watcher<ConfigMap>> aux2 =
            "*".equals(namespace) ? aux1.inAnyNamespace() : aux1.inNamespace(namespace);
        return aux2.withLabels(selector)
            .list()
            .getItems()
            .stream()
            .flatMap(item -> {
                try {
                    return Stream.of(convert(item));
                } catch (Exception e) {
                    // ignore this CM
                    return Stream.empty();
                }
            }).collect(Collectors.toSet());
    }
}
