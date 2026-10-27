-- Azure SQL Database Pipeline Metadata & Lock Stored Procedures
-- Server: databaseserver653.database.windows.net
-- Database: data1

-- 1. Table: ADF_Pipeline_Config
CREATE TABLE IF NOT EXISTS [dbo].[ADF_Pipeline_Config] (
    ConfigID INT IDENTITY(1,1) PRIMARY KEY,
    SourceContainer VARCHAR(100) NOT NULL,
    SourceFolder VARCHAR(200) NOT NULL,
    TargetContainer VARCHAR(100) NOT NULL,
    TargetFolder VARCHAR(200) NOT NULL,
    SourceFormat VARCHAR(50) DEFAULT 'csv',
    TargetFormat VARCHAR(50) DEFAULT 'parquet',
    LoadType VARCHAR(50) DEFAULT 'Full',
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT SYSUTCDATETIME(),
    ModifiedDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

-- 2. Stored Procedure: sp_LockPipeline
CREATE OR ALTER PROCEDURE [dbo].[sp_LockPipeline]
    @PipelineName VARCHAR(100),
    @PipelineRunId VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [dbo].[Pipeline_Lock] (PipelineName, PipelineRunId, LockStatus, LockAcquiredTime)
    VALUES (@PipelineName, @PipelineRunId, 'LOCKED', SYSUTCDATETIME());
    SELECT 1 AS IsLocked;
END;
GO

-- 3. Stored Procedure: sp_UnlockPipeline
CREATE OR ALTER PROCEDURE [dbo].[sp_UnlockPipeline]
    @PipelineName VARCHAR(100),
    @PipelineRunId VARCHAR(100),
    @Status VARCHAR(50),
    @RowsProcessed INT = 0,
    @RowsFailed INT = 0,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[Pipeline_Lock]
    SET LockStatus = 'UNLOCKED',
        LockReleasedTime = SYSUTCDATETIME(),
        FinalStatus = @Status,
        RowsProcessed = @RowsProcessed,
        RowsFailed = @RowsFailed,
        ErrorMessage = @ErrorMessage
    WHERE PipelineName = @PipelineName AND PipelineRunId = @PipelineRunId;
    SELECT 1 AS IsUnlocked;
END;
GO
