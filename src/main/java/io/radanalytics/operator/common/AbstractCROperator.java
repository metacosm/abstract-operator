package io.radanalytics.operator.common;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.radanalytics.operator.common.crd.CrdDeployer;
import io.radanalytics.operator.common.crd.InfoClass;
import io.radanalytics.operator.common.crd.InfoClassDoneable;
import io.radanalytics.operator.common.crd.InfoList;
import io.radanalytics.operator.common.crd.InfoStatus;

/**
 * @author <a href="claprun@redhat.com">Christophe Laprun</a>
 */
public abstract class AbstractCROperator<T extends EntityInfo> extends AbstractOperator<T> {
    @Inject
    private CrdDeployer crdDeployer;
    
    protected Class<? extends EntityInfo>[] secondaryResources;
    protected String[] additionalPrinterColumnNames;
    protected String[] additionalPrinterColumnPaths;
    protected String[] additionalPrinterColumnTypes;
    private CustomResourceDefinition crd;
    
    public AbstractCROperator() {
        Operator annotation = getClass().getAnnotation(Operator.class);
        if (annotation != null) {
            this.additionalPrinterColumnNames = annotation.additionalPrinterColumnNames();
            this.additionalPrinterColumnPaths = annotation.additionalPrinterColumnPaths();
            this.additionalPrinterColumnTypes = annotation.additionalPrinterColumnTypes();
        }
    }
    
    @Override
    protected boolean checkIntegrity() {
        final boolean ok = super.checkIntegrity();
        return ok && additionalPrinterColumnNames == null ||
            (additionalPrinterColumnPaths != null && (additionalPrinterColumnNames.length == additionalPrinterColumnPaths.length)
                && (additionalPrinterColumnTypes == null || additionalPrinterColumnNames.length == additionalPrinterColumnTypes.length));
    }
    
    @Override
    protected void onInit() {
        super.onInit();
        this.crd = crdDeployer.initCrds(client,
            prefix,
            entityName,
            shortNames,
            pluralName,
            additionalPrinterColumnNames,
            additionalPrinterColumnPaths,
            additionalPrinterColumnTypes,
            infoClass,
            isOpenshift);
    }
    
    @Override
    protected CompletableFuture<? extends AbstractWatcher<T>> initializeWatcher() {
        CustomResourceWatcher.Builder<T> crBuilder = new CustomResourceWatcher.Builder<>();
        CustomResourceWatcher crWatcher = crBuilder.withClient(client)
            .withCrd(crd)
            .withEntityName(entityName)
            .withNamespace(namespace)
            .withConvert(this::convertCr)
            .withOnAdd(this::onAdd)
            .withOnDelete(this::onDelete)
            .withOnModify(this::onModify)
            .build();
        return crWatcher.watch();
    }
    
    @Override
    protected Set<T> getDesiredSet() {
        MixedOperation<InfoClass, InfoList, InfoClassDoneable, Resource<InfoClass, InfoClassDoneable>> aux1 =
            client.customResources(crd, InfoClass.class, InfoList.class, InfoClassDoneable.class);
        FilterWatchListMultiDeletable<InfoClass, InfoList, Boolean, Watch, Watcher<InfoClass>> aux2 =
            "*".equals(namespace) ? aux1.inAnyNamespace() : aux1.inNamespace(namespace);
        CustomResourceList<InfoClass> listAux = aux2.list();
        List<InfoClass> items = listAux.getItems();
        return items.stream().flatMap(item -> {
            try {
                return Stream.of(convertCr(item));
            } catch (Exception e) {
                // ignore this CR
                return Stream.empty();
            }
        }).collect(Collectors.toSet());
    }
    
    /**
     * Sets the 'state' field in the status block of the CR identified by namespace and name.
     * The status block in the CR has another component 'lastTransitionTime' which is set
     * automatically. Note, this only works for custom resource watchers, it has no effect for configmap watchers.
     *
     * @param status    String value that will be assigned to the 'state' field in the CR status block
     * @param namespace The namespace holding the CR to update
     * @param name      The name of the CR to update
     **/
    protected void setCRStatus(String status, String namespace, String name) {
        MixedOperation<InfoClass, InfoList, InfoClassDoneable, Resource<InfoClass, InfoClassDoneable>> crclient =
            client.customResources(crd, InfoClass.class, InfoList.class, InfoClassDoneable.class);
        
        InfoClass cr = crclient.inNamespace(namespace).withName(name).get();
        if (cr != null) {
            cr.setStatus(new InfoStatus(status, new Date()));
            crclient.inNamespace(namespace).withName(name).updateStatus(cr);
        }
    }
}
