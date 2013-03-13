/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.ImportClientFactoryFactory;
import com.facebook.presto.split.ImportClientManager;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.fromProperties;

@ThreadSafe
public class PluginManager
{
    private static final Logger log = Logger.get(PluginManager.class);

    private final Injector injector;
    private final ImportClientManager importClientManager;
    private final ArtifactResolver resolver;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final File pluginConfigurationDir;
    private final Map<String, String> optionalConfig;
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public PluginManager(Injector injector, 
            NodeInfo nodeInfo,
            HttpServerInfo httpServerInfo,
            PluginManagerConfig config,
            ImportClientManager importClientManager,
            ConfigurationFactory configurationFactory)
    {
        checkNotNull(injector, "injector is null");
        checkNotNull(nodeInfo, "nodeInfo is null");
        checkNotNull(httpServerInfo, "httpServerInfo is null");
        checkNotNull(config, "config is null");
        checkNotNull(importClientManager, "importClientManager is null");
        checkNotNull(configurationFactory, "configurationFactory is null");

        this.injector = injector;
        this.importClientManager = importClientManager;
        installedPluginsDir = config.getInstalledPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.pluginConfigurationDir = config.getPluginConfigurationDir();
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());

        Map<String, String> optionalConfig = new TreeMap<>(configurationFactory.getProperties());
        optionalConfig.put("node.id", nodeInfo.getNodeId());
        // TODO: make this work with and without HTTP and HTTPS
        optionalConfig.put("http-server.http.port", Integer.toString(httpServerInfo.getHttpUri().getPort()));
        this.optionalConfig = ImmutableMap.copyOf(optionalConfig);
    }

    public boolean arePluginsLoaded()
    {
        return pluginsLoaded.get();
    }

    public void loadPlugins()
            throws Exception
    {
        if (!pluginsLoaded.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(installedPluginsDir)) {
            if (file.isDirectory()) {
                loadPlugin(file.getAbsolutePath());
            }
        }

        for (String plugin : plugins) {
            loadPlugin(plugin);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    private void loadPlugin(String plugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(plugin);
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(pluginClassLoader)) {
            ServiceLoader<ImportClientFactoryFactory> serviceLoader = ServiceLoader.load(ImportClientFactoryFactory.class, pluginClassLoader);
            List<ImportClientFactoryFactory> importClientFactoryFactories = ImmutableList.copyOf(serviceLoader);

            for (ImportClientFactoryFactory importClientFactoryFactory : importClientFactoryFactories) {
                if (importClientFactoryFactory.getClass().isAnnotationPresent(PrestoInternalPlugin.class)) {
                    injector.injectMembers(importClientFactoryFactory);
                }
                Map<String, String> requiredConfig = loadPluginConfig(importClientFactoryFactory.getConfigName());
                ImportClientFactory importClientFactory = importClientFactoryFactory.createImportClientFactory(requiredConfig, optionalConfig);
                importClientFactory = new ClassLoaderSafeImportClientFactory(importClientFactory, pluginClassLoader);
                importClientManager.addImportClientFactory(importClientFactory);
            }
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private Map<String, String> loadPluginConfig(String name)
            throws Exception
    {
        checkNotNull(name, "name is null");

        Properties properties = new Properties();
        if (pluginConfigurationDir != null) {
            File configFile = new File(pluginConfigurationDir, name + ".properties");
            if (configFile.canRead()) {
                try (FileInputStream in = new FileInputStream(configFile)) {
                    properties.load(in);
                }
            }
        }
        return fromProperties(properties);
    }

    private URLClassLoader buildClassLoader(String plugin)
            throws MalformedURLException
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file);
        }
        else if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        else {
            return buildClassLoaderFromCoordinates(plugin);
        }
    }

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws MalformedURLException
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);

        log.debug("Classpath for %s:", pomFile);
        List<URL> urls = new ArrayList<>();
        urls.add(new File(pomFile.getParentFile(), "target/classes/").toURI().toURL());
        for (Artifact artifact : artifacts) {
            if (artifact.getFile() != null) {
                log.debug("    %s", artifact.getFile());
                urls.add(artifact.getFile().toURI().toURL());
            }
            else {
                log.debug("  Could not resolve artifact %s", artifact);
            }
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws MalformedURLException
    {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromCoordinates(String coordinates)
            throws MalformedURLException
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);

        log.debug("Classpath for %s:", rootArtifact);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : artifacts) {
            if (artifact.getFile() != null) {
                log.debug("    %s", artifact.getFile());
                urls.add(artifact.getFile().toURI().toURL());
            }
            else {
                // todo maybe exclude things like presto-spi
                log.warn("  Could not resolve artifact %s", artifact);
            }
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        return new SimpleChildFirstClassLoader(urls,
                getClass().getClassLoader(),
                ImmutableList.of("org.slf4j"),
                ImmutableList.of("com.facebook.presto"));
    }

    private List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static class SimpleChildFirstClassLoader
            extends URLClassLoader
    {
        private final List<String> hiddenClasses;
        private final List<String> parentFirstClasses;
        private final List<String> hiddenResources;
        private final List<String> parentFirstResources;

        public SimpleChildFirstClassLoader(List<URL> urls,
                ClassLoader parent,
                Iterable<String> hiddenClasses,
                Iterable<String> parentFirstClasses)
        {
            this(urls,
                    parent,
                    hiddenClasses,
                    parentFirstClasses,
                    Iterables.transform(hiddenClasses, classNameToResource()),
                    Iterables.transform(parentFirstClasses, classNameToResource()));
        }

        public SimpleChildFirstClassLoader(List<URL> urls,
                ClassLoader parent,
                Iterable<String> hiddenClasses,
                Iterable<String> parentFirstClasses,
                Iterable<String> hiddenResources,
                Iterable<String> parentFirstResources)
        {
            // child first requires a parent class loader
            super(urls.toArray(new URL[urls.size()]), checkNotNull(parent, "parent is null"));
            this.hiddenClasses = ImmutableList.copyOf(hiddenClasses);
            this.parentFirstClasses = ImmutableList.copyOf(parentFirstClasses);
            this.hiddenResources = ImmutableList.copyOf(hiddenResources);
            this.parentFirstResources = ImmutableList.copyOf(parentFirstResources);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException
        {
            // grab the magic lock
            synchronized (getClassLoadingLock(name)) {
                // Check if class is in the loaded classes cache
                Class<?> cachedClass = findLoadedClass(name);
                if (cachedClass != null) {
                    return resolveClass(cachedClass, resolve);
                }

                // If this is not a parent first class, look for the class locally
                if (!isParentFirstClass(name)) {
                    try {
                        Class<?> clazz = findClass(name);
                        return resolveClass(clazz, resolve);
                    }
                    catch (ClassNotFoundException ignored) {
                        // not a local class
                    }
                }

                // Check parent class loaders, unless this is a hidden class
                if (!isHiddenClass(name)) {
                    try {
                        Class<?> clazz = getParent().loadClass(name);
                        return resolveClass(clazz, resolve);
                    }
                    catch (ClassNotFoundException ignored) {
                        // this parent didn't have the class
                    }
                }

                // If this is a parent first class, now look for the class locally
                if (isParentFirstClass(name)) {
                    Class<?> clazz = findClass(name);
                    return resolveClass(clazz, resolve);
                }

                throw new ClassNotFoundException(name);
            }
        }

        private Class<?> resolveClass(Class<?> clazz, boolean resolve)
        {
            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        }

        private boolean isParentFirstClass(String name)
        {
            for (String nonOverridableClass : parentFirstClasses) {
                // todo maybe make this more precise and only match base package
                if (name.startsWith(nonOverridableClass)) {
                    return true;
                }
            }
            return false;
        }

        private boolean isHiddenClass(String name)
        {
            for (String hiddenClass : hiddenClasses) {
                // todo maybe make this more precise and only match base package
                if (name.startsWith(hiddenClass)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public URL getResource(String name)
        {
            // If this is not a parent first resource, check local resources first
            if (!isParentFirstResource(name)) {
                URL url = findResource(name);
                if (url != null) {
                    return url;
                }
            }

            // Check parent class loaders
            if (!isHiddenResource(name)) {
                URL url = getParent().getResource(name);
                if (url != null) {
                    return url;
                }
            }

            // If this is a parent first resource, now check local resources
            if (isParentFirstResource(name)) {
                URL url = findResource(name);
                if (url != null) {
                    return url;
                }
            }

            return null;
        }

        @Override
        public Enumeration<URL> getResources(String name)
                throws IOException
        {
            List<Iterator<URL>> resources = new ArrayList<>();

            // If this is not a parent first resource, add resources from local urls first
            if (!isParentFirstResource(name)) {
                Iterator<URL> myResources = Iterators.forEnumeration(findResources(name));
                resources.add(myResources);
            }

            // Add parent resources
            if (!isHiddenResource(name)) {
                Iterator<URL> parentResources = Iterators.forEnumeration(getParent().getResources(name));
                resources.add(parentResources);
            }

            // If this is a parent first resource, now add resources from local urls
            if (isParentFirstResource(name)) {
                Iterator<URL> myResources = Iterators.forEnumeration(findResources(name));
                resources.add(myResources);
            }

            return Iterators.asEnumeration(Iterators.concat(resources.iterator()));
        }

        private boolean isParentFirstResource(String name)
        {
            for (String nonOverridableResource : parentFirstResources) {
                if (name.startsWith(nonOverridableResource)) {
                    return true;
                }
            }
            return false;
        }

        private boolean isHiddenResource(String name)
        {
            for (String hiddenResource : hiddenResources) {
                if (name.startsWith(hiddenResource)) {
                    return true;
                }
            }
            return false;
        }

        private static Function<String, String> classNameToResource()
        {
            return new Function<String, String>()
            {
                @Override
                public String apply(String className)
                {
                    return className.replace('.', '/');
                }
            };
        }
    }
}
