package org.apache.spark.loader;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.CompoundEnumeration;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;


public class DtClassLoader extends URLClassLoader {

    private static Logger log = LoggerFactory.getLogger(DtClassLoader.class);

    private static final String CLASS_FILE_SUFFIX = ".class";

    private boolean hasExternalRepositories = false;

    /**
     * The parent class loader.
     */
    protected ClassLoader parent;

    public DtClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.parent = parent;
    }



    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        String classPath = this.getResource("/").getPath(); //得到classpath
        String fileName = name.replace(".", "/") + CLASS_FILE_SUFFIX;
        File classFile = new File(classPath, fileName);
        if (!classFile.exists()) {
            throw new ClassNotFoundException(classFile.getPath() + " not exist");
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteBuffer bf = ByteBuffer.allocate(1024);
        FileInputStream fis = null;
        FileChannel fc = null;
        try {
            fis = new FileInputStream(classFile);
            fc = fis.getChannel();
            while (fc.read(bf) > 0) {
                bf.flip();
                bos.write(bf.array(), 0, bf.limit());
                bf.clear();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
                fc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return defineClass(bos.toByteArray(), 0, bos.toByteArray().length);
    }

    @Override
    public URL getResource(String name) {

        if (log.isDebugEnabled()){
            log.debug("getResource(" + name + ")");
        }

        URL url = null;

        // (2) Search local repositories
        url = findResource(name);
        if (url != null) {
            if (log.isDebugEnabled()){
                log.debug("  --> Returning '" + url.toString() + "'");
            }
            return (url);
        }

        // (3) Delegate to parent unconditionally if not already attempted
        url = parent.getResource(name);
        if (url != null) {
            if (log.isDebugEnabled()){
                log.debug("  --> Returning '" + url.toString() + "'");
            }
            return (url);
        }

        // (4) Resource was not found
        if (log.isDebugEnabled()){
            log.debug("  --> Resource not found, returning null");
        }
        return (null);
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
        hasExternalRepositories = true;
    }

    /**
     * FIXME 需要测试
     * @param name
     * @return
     * @throws IOException
     */
    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        @SuppressWarnings("unchecked")
        Enumeration<URL>[] tmp = (Enumeration<URL>[]) new Enumeration<?>[1];
        tmp[0] = findResources(name);//优先使用当前类的资源

        if(!tmp[0].hasMoreElements()){//只有子classLoader找不到任何资源才会调用原生的方法
            return super.getResources(name);
        }

        return new CompoundEnumeration<>(tmp);
    }

    @Override
    public Enumeration<URL> findResources(String name) throws IOException {

        if (log.isDebugEnabled()){
            log.debug("findResources(" + name + ")");
        }

        LinkedHashSet<URL> result = new LinkedHashSet<>();

        Enumeration<URL> superResource = super.findResources(name);

        while (superResource.hasMoreElements()){
            result.add(superResource.nextElement());
        }

        // Adding the results of a call to the superclass
        if (hasExternalRepositories) {
            Enumeration<URL> otherResourcePaths = super.findResources(name);
            while (otherResourcePaths.hasMoreElements()) {
                result.add(otherResourcePaths.nextElement());
            }
        }

        return Collections.enumeration(result);
    }
}