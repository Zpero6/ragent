/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.knowledge.schedule;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.nageoffer.ai.ragent.framework.context.LoginUser;
import com.nageoffer.ai.ragent.framework.context.UserContext;
import com.nageoffer.ai.ragent.framework.exception.ClientException;
import com.nageoffer.ai.ragent.knowledge.config.KnowledgeScheduleProperties;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeBaseDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentScheduleDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentScheduleExecDO;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentScheduleExecMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentScheduleMapper;
import com.nageoffer.ai.ragent.knowledge.enums.DocumentStatus;
import com.nageoffer.ai.ragent.knowledge.enums.ScheduleRunStatus;
import com.nageoffer.ai.ragent.knowledge.enums.SourceType;
import com.nageoffer.ai.ragent.knowledge.handler.RemoteFileFetcher;
import com.nageoffer.ai.ragent.knowledge.service.impl.KnowledgeDocumentServiceImpl;
import com.nageoffer.ai.ragent.rag.dto.StoredFileDTO;
import com.nageoffer.ai.ragent.rag.service.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * 知识库文档定时刷新任务
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KnowledgeDocumentScheduleJob {

    private static final String SYSTEM_USER = "system";

    private final KnowledgeDocumentScheduleMapper scheduleMapper;
    private final KnowledgeDocumentScheduleExecMapper execMapper;
    private final KnowledgeDocumentMapper documentMapper;
    private final KnowledgeBaseMapper kbMapper;
    private final KnowledgeDocumentServiceImpl documentService;
    private final FileStorageService fileStorageService;
    private final RemoteFileFetcher remoteFileFetcher;
    private final Executor knowledgeChunkExecutor;
    private final KnowledgeScheduleProperties scheduleProperties;

    private final String instanceId = resolveInstanceId();

    @Scheduled(fixedDelayString = "${rag.knowledge.schedule.scan-delay-ms:10000}")
    public void scan() {
        Date now = new Date();
        List<KnowledgeDocumentScheduleDO> schedules = scheduleMapper.selectList(
                new LambdaQueryWrapper<KnowledgeDocumentScheduleDO>()
                        .eq(KnowledgeDocumentScheduleDO::getEnabled, 1)
                        .and(wrapper -> wrapper.isNull(KnowledgeDocumentScheduleDO::getNextRunTime)
                                .or()
                                .le(KnowledgeDocumentScheduleDO::getNextRunTime, now))
                        .and(wrapper -> wrapper.isNull(KnowledgeDocumentScheduleDO::getLockUntil)
                                .or()
                                .lt(KnowledgeDocumentScheduleDO::getLockUntil, now))
                        .orderByAsc(KnowledgeDocumentScheduleDO::getNextRunTime)
                        .last("LIMIT " + Math.max(scheduleProperties.getBatchSize(), 1))
        );

        if (schedules == null || schedules.isEmpty()) {
            return;
        }

        Date lockUntil = new Date(System.currentTimeMillis() + Math.max(scheduleProperties.getLockSeconds(), 60) * 1000);
        for (KnowledgeDocumentScheduleDO schedule : schedules) {
            if (schedule == null || schedule.getId() == null) {
                continue;
            }
            if (!tryAcquireLock(schedule.getId(), now, lockUntil)) {
                continue;
            }
            try {
                knowledgeChunkExecutor.execute(() -> executeSchedule(schedule.getId()));
            } catch (RejectedExecutionException e) {
                log.error("定时任务提交失败: scheduleId={}, docId={}, kbId={}",
                        schedule.getId(), schedule.getDocId(), schedule.getKbId(), e);
                releaseLock(schedule.getId());
            }
        }
    }

    private void executeSchedule(String scheduleId) {
        Date startTime = new Date();
        KnowledgeDocumentScheduleDO schedule = scheduleMapper.selectById(scheduleId);
        if (schedule == null) {
            return;
        }
        renewLock(scheduleId);

        KnowledgeDocumentDO document = documentMapper.selectById(schedule.getDocId());
        if (document == null || (document.getDeleted() != null && document.getDeleted() == 1)) {
            disableSchedule(schedule, "文档不存在或已删除");
            releaseLock(scheduleId);
            return;
        }
        if (document.getEnabled() != null && document.getEnabled() == 0) {
            disableSchedule(schedule, "文档已禁用");
            releaseLock(scheduleId);
            return;
        }

        String cron = document.getScheduleCron();
        boolean enabled = document.getScheduleEnabled() != null && document.getScheduleEnabled() == 1;
        if (!StringUtils.hasText(cron) || !SourceType.URL.getValue().equalsIgnoreCase(document.getSourceType())) {
            enabled = false;
        }

        schedule.setCronExpr(cron);
        Date nextRunTime;
        if (enabled) {
            try {
                nextRunTime = CronScheduleHelper.nextRunTime(cron, startTime);
            } catch (IllegalArgumentException e) {
                disableSchedule(schedule, "定时表达式不合法");
                releaseLock(scheduleId);
                return;
            }
            if (nextRunTime == null) {
                disableSchedule(schedule, "无法计算下次执行时间");
                releaseLock(scheduleId);
                return;
            }
        } else {
            disableSchedule(schedule, "定时已关闭");
            releaseLock(scheduleId);
            return;
        }

        KnowledgeDocumentScheduleExecDO exec = KnowledgeDocumentScheduleExecDO.builder()
                .scheduleId(scheduleId)
                .docId(document.getId())
                .kbId(document.getKbId())
                .status(ScheduleRunStatus.RUNNING.getCode())
                .startTime(startTime)
                .build();
        execMapper.insert(exec);

        String oldFileUrl = null;
        StoredFileDTO stored = null;
        boolean documentOccupied = false;
        boolean switchedToNewFile = false;
        try (RemoteFileFetcher.RemoteFetchResult fetchResult = remoteFileFetcher.fetchIfChanged(
                document.getSourceLocation(),
                schedule.getLastEtag(),
                schedule.getLastModified(),
                schedule.getLastContentHash(),
                document.getDocName()
        )) {
            if (!fetchResult.changed()) {
                markScheduleSkipped(schedule, exec.getId(), startTime, nextRunTime, fetchResult);
                return;
            }

            if (DocumentStatus.RUNNING.getCode().equals(document.getStatus())) {
                markScheduleSkipped(schedule, exec.getId(), startTime, nextRunTime, "文档正在分块中，跳过本次调度");
                return;
            }

            renewLock(scheduleId);
            if (!tryMarkDocumentRunning(document.getId())) {
                markScheduleSkipped(schedule, exec.getId(), startTime, nextRunTime, "文档正在分块中，跳过本次调度");
                return;
            }
            documentOccupied = true;

            KnowledgeBaseDO kbDO = kbMapper.selectById(document.getKbId());
            if (kbDO == null) {
                throw new ClientException("知识库不存在");
            }

            oldFileUrl = document.getFileUrl();
            try (InputStream tempIn = Files.newInputStream(fetchResult.tempFile())) {
                stored = fileStorageService.upload(
                        kbDO.getCollectionName(),
                        tempIn,
                        fetchResult.size(),
                        fetchResult.fileName(),
                        fetchResult.contentType()
                );
            }

            KnowledgeDocumentDO runtimeDoc = documentMapper.selectById(document.getId());
            if (runtimeDoc == null) {
                throw new ClientException("文档不存在");
            }
            runtimeDoc.setDocName(stored.getOriginalFilename());
            runtimeDoc.setFileUrl(stored.getUrl());
            runtimeDoc.setFileType(stored.getDetectedType());
            runtimeDoc.setFileSize(stored.getSize());
            runtimeDoc.setUpdatedBy(SYSTEM_USER);

            renewLock(scheduleId);
            UserContext.set(LoginUser.builder().username(SYSTEM_USER).build());
            try {
                documentService.chunkDocument(runtimeDoc);
            } finally {
                UserContext.clear();
            }

            KnowledgeDocumentDO latest = documentMapper.selectById(document.getId());
            if (latest == null || !DocumentStatus.SUCCESS.getCode().equals(latest.getStatus())) {
                markScheduleFailed(schedule, exec.getId(), startTime, nextRunTime, "分块失败");
                return;
            }

            applyRefreshedFileMetadata(document.getId(), stored);
            switchedToNewFile = true;

            renewLock(scheduleId);
            markScheduleSuccess(schedule, exec.getId(), startTime, nextRunTime, fetchResult, stored);
        } catch (Exception e) {
            log.error("定时刷新失败: scheduleId={}, docId={}, kbId={}",
                    scheduleId, document.getId(), document.getKbId(), e);
            if (!switchedToNewFile) {
                if (documentOccupied) {
                    markDocumentFailedIfRunning(document.getId());
                }
                markScheduleFailed(schedule, exec.getId(), startTime, nextRunTime, e.getMessage());
            }
        } finally {
            if (switchedToNewFile) {
                deleteOldFileQuietly(oldFileUrl, stored != null ? stored.getUrl() : null);
            } else if (stored != null) {
                deleteOldFileQuietly(stored.getUrl(), null);
            }
            releaseLock(scheduleId);
        }
    }

    private void markScheduleSkipped(KnowledgeDocumentScheduleDO schedule,
                                     String execId,
                                     Date startTime,
                                     Date nextRunTime,
                                     RemoteFileFetcher.RemoteFetchResult fetchResult) {
        Date endTime = new Date();
        KnowledgeDocumentScheduleDO update = new KnowledgeDocumentScheduleDO();
        update.setId(schedule.getId());
        update.setCronExpr(schedule.getCronExpr());
        update.setLastRunTime(startTime);
        update.setNextRunTime(nextRunTime);
        update.setLastStatus(ScheduleRunStatus.SKIPPED.getCode());
        update.setLastError(fetchResult.message());
        update.setLastEtag(fetchResult.etag());
        update.setLastModified(fetchResult.lastModified());
        update.setLastContentHash(fetchResult.contentHash());
        scheduleMapper.updateById(update);

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = new KnowledgeDocumentScheduleExecDO();
            execUpdate.setId(execId);
            execUpdate.setStatus(ScheduleRunStatus.SKIPPED.getCode());
            execUpdate.setMessage(fetchResult.message());
            execUpdate.setEndTime(endTime);
            execUpdate.setContentHash(fetchResult.contentHash());
            execUpdate.setEtag(fetchResult.etag());
            execUpdate.setLastModified(fetchResult.lastModified());
            execMapper.updateById(execUpdate);
        }
    }

    private void markScheduleSkipped(KnowledgeDocumentScheduleDO schedule,
                                     String execId,
                                     Date startTime,
                                     Date nextRunTime,
                                     String message) {
        Date endTime = new Date();
        KnowledgeDocumentScheduleDO update = new KnowledgeDocumentScheduleDO();
        update.setId(schedule.getId());
        update.setCronExpr(schedule.getCronExpr());
        update.setLastRunTime(startTime);
        update.setNextRunTime(nextRunTime);
        update.setLastStatus(ScheduleRunStatus.SKIPPED.getCode());
        update.setLastError(message);
        scheduleMapper.updateById(update);

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = new KnowledgeDocumentScheduleExecDO();
            execUpdate.setId(execId);
            execUpdate.setStatus(ScheduleRunStatus.SKIPPED.getCode());
            execUpdate.setMessage(message);
            execUpdate.setEndTime(endTime);
            execMapper.updateById(execUpdate);
        }
    }

    private void markScheduleSuccess(KnowledgeDocumentScheduleDO schedule,
                                     String execId,
                                     Date startTime,
                                     Date nextRunTime,
                                     RemoteFileFetcher.RemoteFetchResult fetchResult,
                                     StoredFileDTO stored) {
        Date endTime = new Date();
        scheduleMapper.update(
                null,
                new LambdaUpdateWrapper<KnowledgeDocumentScheduleDO>()
                        .set(KnowledgeDocumentScheduleDO::getCronExpr, schedule.getCronExpr())
                        .set(KnowledgeDocumentScheduleDO::getLastRunTime, startTime)
                        .set(KnowledgeDocumentScheduleDO::getNextRunTime, nextRunTime)
                        .set(KnowledgeDocumentScheduleDO::getLastSuccessTime, endTime)
                        .set(KnowledgeDocumentScheduleDO::getLastStatus, ScheduleRunStatus.SUCCESS.getCode())
                        .set(KnowledgeDocumentScheduleDO::getLastError, null)
                        .set(KnowledgeDocumentScheduleDO::getLastEtag, fetchResult.etag())
                        .set(KnowledgeDocumentScheduleDO::getLastModified, fetchResult.lastModified())
                        .set(KnowledgeDocumentScheduleDO::getLastContentHash, fetchResult.contentHash())
                        .eq(KnowledgeDocumentScheduleDO::getId, schedule.getId())
        );

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = new KnowledgeDocumentScheduleExecDO();
            execUpdate.setId(execId);
            execUpdate.setStatus(ScheduleRunStatus.SUCCESS.getCode());
            execUpdate.setMessage("刷新成功");
            execUpdate.setEndTime(endTime);
            execUpdate.setFileName(stored.getOriginalFilename());
            execUpdate.setFileSize(stored.getSize());
            execUpdate.setContentHash(fetchResult.contentHash());
            execUpdate.setEtag(fetchResult.etag());
            execUpdate.setLastModified(fetchResult.lastModified());
            execMapper.updateById(execUpdate);
        }
    }

    private void markScheduleFailed(KnowledgeDocumentScheduleDO schedule,
                                    String execId,
                                    Date startTime,
                                    Date nextRunTime,
                                    String errorMessage) {
        Date endTime = new Date();
        KnowledgeDocumentScheduleDO update = new KnowledgeDocumentScheduleDO();
        update.setId(schedule.getId());
        update.setCronExpr(schedule.getCronExpr());
        update.setLastRunTime(startTime);
        update.setNextRunTime(nextRunTime);
        update.setLastStatus(ScheduleRunStatus.FAILED.getCode());
        update.setLastError(truncate(errorMessage));
        scheduleMapper.updateById(update);

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = new KnowledgeDocumentScheduleExecDO();
            execUpdate.setId(execId);
            execUpdate.setStatus(ScheduleRunStatus.FAILED.getCode());
            execUpdate.setMessage(truncate(errorMessage));
            execUpdate.setEndTime(endTime);
            execMapper.updateById(execUpdate);
        }
    }

    private void disableSchedule(KnowledgeDocumentScheduleDO schedule, String reason) {
        scheduleMapper.update(
                null,
                new LambdaUpdateWrapper<KnowledgeDocumentScheduleDO>()
                        .set(KnowledgeDocumentScheduleDO::getEnabled, 0)
                        .set(KnowledgeDocumentScheduleDO::getNextRunTime, null)
                        .set(KnowledgeDocumentScheduleDO::getLastStatus, ScheduleRunStatus.FAILED.getCode())
                        .set(KnowledgeDocumentScheduleDO::getLastError, truncate(reason))
                        .eq(KnowledgeDocumentScheduleDO::getId, schedule.getId())
        );
    }

    private boolean tryAcquireLock(String scheduleId, Date now, Date lockUntil) {
        UpdateWrapper<KnowledgeDocumentScheduleDO> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", scheduleId)
                .and(wrapper -> wrapper.isNull("lock_until").or().lt("lock_until", now));
        KnowledgeDocumentScheduleDO update = new KnowledgeDocumentScheduleDO();
        update.setLockOwner(instanceId);
        update.setLockUntil(lockUntil);
        return scheduleMapper.update(update, updateWrapper) > 0;
    }

    private void releaseLock(String scheduleId) {
        scheduleMapper.update(
                null,
                new LambdaUpdateWrapper<KnowledgeDocumentScheduleDO>()
                        .set(KnowledgeDocumentScheduleDO::getLockOwner, null)
                        .set(KnowledgeDocumentScheduleDO::getLockUntil, null)
                        .eq(KnowledgeDocumentScheduleDO::getId, scheduleId)
                        .eq(KnowledgeDocumentScheduleDO::getLockOwner, instanceId)
        );
    }

    private void renewLock(String scheduleId) {
        if (scheduleId == null) {
            return;
        }
        Date lockUntil = new Date(System.currentTimeMillis() + Math.max(scheduleProperties.getLockSeconds(), 60) * 1000);
        UpdateWrapper<KnowledgeDocumentScheduleDO> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", scheduleId).eq("lock_owner", instanceId);
        KnowledgeDocumentScheduleDO update = new KnowledgeDocumentScheduleDO();
        update.setLockUntil(lockUntil);
        scheduleMapper.update(update, updateWrapper);
    }

    private boolean tryMarkDocumentRunning(String docId) {
        UpdateWrapper<KnowledgeDocumentDO> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", docId)
                .eq("deleted", 0)
                .eq("enabled", 1)
                .ne("status", DocumentStatus.RUNNING.getCode());
        KnowledgeDocumentDO update = new KnowledgeDocumentDO();
        update.setStatus(DocumentStatus.RUNNING.getCode());
        update.setUpdatedBy(SYSTEM_USER);
        return documentMapper.update(update, updateWrapper) > 0;
    }

    private void markDocumentFailedIfRunning(String docId) {
        UpdateWrapper<KnowledgeDocumentDO> updateWrapper = new UpdateWrapper<>();
        updateWrapper.eq("id", docId)
                .eq("status", DocumentStatus.RUNNING.getCode());
        KnowledgeDocumentDO update = new KnowledgeDocumentDO();
        update.setStatus(DocumentStatus.FAILED.getCode());
        update.setUpdatedBy(SYSTEM_USER);
        documentMapper.update(update, updateWrapper);
    }

    private void applyRefreshedFileMetadata(String docId, StoredFileDTO stored) {
        KnowledgeDocumentDO update = new KnowledgeDocumentDO();
        update.setId(docId);
        update.setDocName(stored.getOriginalFilename());
        update.setFileUrl(stored.getUrl());
        update.setFileType(stored.getDetectedType());
        update.setFileSize(stored.getSize());
        update.setUpdatedBy(SYSTEM_USER);
        int updated = documentMapper.updateById(update);
        if (updated == 0) {
            throw new ClientException("文档不存在");
        }
    }

    private void deleteOldFileQuietly(String oldFileUrl, String newFileUrl) {
        if (!StringUtils.hasText(oldFileUrl) || oldFileUrl.equals(newFileUrl)) {
            return;
        }
        try {
            fileStorageService.deleteByUrl(oldFileUrl);
        } catch (Exception e) {
            log.warn("定时刷新文件清理失败: {}", oldFileUrl, e);
        }
    }

    private String resolveInstanceId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        return "kb-schedule-" + host + "-" + UUID.randomUUID();
    }

    private String truncate(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        String trimmed = value.trim();
        if (trimmed.length() <= 512) {
            return trimmed;
        }
        return trimmed.substring(0, 512);
    }
}
