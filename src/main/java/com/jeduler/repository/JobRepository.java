package com.jeduler.repository;

import com.jeduler.model.entity.Job;
import com.jeduler.model.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface JobRepository extends JpaRepository<Job, Long> {

    @Query(value = """
        SELECT j.* FROM jfc_job j
        WHERE j.status IN ('WAITING', 'RETRY')
        AND j.job_name = :jobName
        ORDER BY j.priority ASC, j.submit_time ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<Job> findSchedulableCandidates(@Param("jobName") String jobName, @Param("limit") int limit);

    @Query(value = """
        SELECT j.* FROM jfc_job j
        WHERE j.status IN ('WAITING', 'RETRY')
        AND j.job_name = :jobName
        AND NOT (j.concurrency_control @> ANY(ARRAY[:excludedJsonPatterns]::jsonb[]))
        ORDER BY j.priority ASC, j.submit_time ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<Job> findSchedulableCandidatesExcluding(
        @Param("jobName") String jobName,
        @Param("excludedJsonPatterns") List<String> excludedJsonPatterns,
        @Param("limit") int limit);

    @Query("SELECT j FROM Job j WHERE j.status = :status")
    List<Job> findByStatus(@Param("status") JobStatus status);

    @Query("SELECT j FROM Job j WHERE j.status = :status AND j.jobName = :jobName")
    List<Job> findByStatusAndJobName(@Param("status") JobStatus status, @Param("jobName") String jobName);

    @Query("SELECT DISTINCT j.jobName FROM Job j WHERE j.status IN :statuses")
    List<String> findDistinctJobNamesByStatusIn(@Param("statuses") List<JobStatus> statuses);

    @Query(value = """
        SELECT j.job_name, j.status, COUNT(*) as cnt
        FROM jfc_job j
        GROUP BY j.job_name, j.status
        """, nativeQuery = true)
    List<Object[]> getJobStats();

    @Query("SELECT COUNT(j) FROM Job j WHERE j.status IN :statuses AND j.jobName = :jobName")
    long countByStatusInAndJobName(@Param("statuses") List<JobStatus> statuses, @Param("jobName") String jobName);

    @Modifying
    @Query("UPDATE Job j SET j.status = :status, j.lastUpdated = CURRENT_TIMESTAMP WHERE j.id IN :ids AND j.status IN :fromStatuses")
    int updateStatusForIds(@Param("ids") List<Long> ids, @Param("status") JobStatus status, @Param("fromStatuses") List<JobStatus> fromStatuses);

    @Modifying
    @Query("UPDATE Job j SET j.priority = :priority, j.lastUpdated = CURRENT_TIMESTAMP WHERE j.id IN :ids AND j.status IN ('WAITING', 'RETRY')")
    int updatePriorityForIds(@Param("ids") List<Long> ids, @Param("priority") int priority);

    @Modifying
    @Query("UPDATE Job j SET j.status = 'CANCELLED', j.lastUpdated = CURRENT_TIMESTAMP WHERE j.tenant = :tenant AND j.jobName = :jobName AND j.status IN ('WAITING', 'RETRY', 'PUBLISHED')")
    int cancelByTenantAndJobName(@Param("tenant") int tenant, @Param("jobName") String jobName);

    @Modifying
    @Query("UPDATE Job j SET j.status = 'CANCELLED', j.lastUpdated = CURRENT_TIMESTAMP WHERE j.tenant = :tenant AND j.status IN ('WAITING', 'RETRY', 'PUBLISHED')")
    int cancelByTenant(@Param("tenant") int tenant);

    @Query(value = """
        SELECT COUNT(*) FROM jfc_job
        WHERE job_name = :jobName
        AND status IN ('PUBLISHED', 'PROCESSING')
        AND concurrency_control->>:ruleKey = :ruleValue
        """, nativeQuery = true)
    long countRunningByRule(@Param("jobName") String jobName, @Param("ruleKey") String ruleKey, @Param("ruleValue") String ruleValue);

    @Query(value = """
        SELECT COUNT(*) FROM jfc_job
        WHERE job_name = :jobName
        AND status IN ('PUBLISHED', 'PROCESSING')
        """, nativeQuery = true)
    long countRunningGlobal(@Param("jobName") String jobName);
}
