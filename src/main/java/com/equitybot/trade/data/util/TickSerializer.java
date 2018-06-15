package com.equitybot.trade.data.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class TickSerializer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${tick.file.Serializer}")
    private String serializeFolder;

    @Value("${tick.maxserializetick}")
    private long maxSerializeTick;

    private long serializeTickCount;
    private File tickFile;

    public synchronized void serializeTick(Tick tick) throws IOException {

        if (this.tickFile == null || !this.tickFile.exists() || this.maxSerializeTick == this.serializeTickCount) {
            this.tickFile = new File(this.serializeFolder + getNewFileName());
            if (!this.tickFile.exists()) {
                this.tickFile.createNewFile();
            }
            this.serializeTickCount = 0;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        Files.write(Paths.get(this.tickFile.getAbsolutePath()), objectMapper.writeValueAsString(tick).getBytes(), StandardOpenOption.APPEND);
        Files.write(Paths.get(this.tickFile.getAbsolutePath()), System.lineSeparator().getBytes(), StandardOpenOption.APPEND);
        serializeTickCount++;
    }

    private String getNewFileName() {
        return "/TickData_" + new SimpleDateFormat("yyyy-MM-dd hh-mm-ss'.txt'").format(new Date());
    }
}
