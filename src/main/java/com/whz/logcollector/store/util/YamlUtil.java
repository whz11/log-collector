package com.whz.logcollector.store.util;

import com.whz.logcollector.LocalRuntimeException;
import com.whz.logcollector.store.config.LogStoreConfig;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * @author whz
 * @date 2022/2/7 11:00
 **/
public class YamlUtil {

    public static <T> T read(String yamlName, Class<T> clazz) {
        InputStream stream = YamlUtil.class.getClassLoader().getResourceAsStream(yamlName);
        if (Objects.nonNull(stream)) {
            Yaml yaml = new Yaml();
            return yaml.loadAs(stream, clazz);
        }
        throw new LocalRuntimeException("parse yaml configuration failed.");
    }

    public static <T> void write(String yamlName, T object) {
        Yaml yaml = new Yaml();
        try {
            String result = yaml.dumpAs(object, Tag.MAP, null);
            FileWriter fileWriter = new FileWriter(yamlName);
            fileWriter.write(result);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(read("logStore-prod.yaml", LogStoreConfig.class));
    }
}
