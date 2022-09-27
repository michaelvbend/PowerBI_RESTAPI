create_staging_fact = """DROP TABLE IF EXISTS [dbo].[FACT_RefreshHistory_stage] 
                         CREATE TABLE [dbo].[FACT_RefreshHistory_stage](
                            [Dataset_ID] [varchar](50) NOT NULL,
                            [request_id] [varchar](50) NOT NULL,
                            [refreshType] [nvarchar](30) NOT NULL,
                            [startTime] [datetime] NOT NULL,
                            [endtime] [datetime] NULL,
                            [status] [varchar](30) NULL,
                            [milestone] datetime null

                        ) """

create_staging_dim = """DROP TABLE IF EXISTS [dbo].[DIM_Dataset_stage] 
                         CREATE TABLE [dbo].[DIM_Dataset_stage](
                            [Dataset_ID] [varchar](50) NOT NULL,
                            [Dataset_Name] [varchar](50) NOT NULL,
                            [Created_date] [date] NOT NULL,
                            [Description] [varchar](100) NULL,
                            [Dataset_owner] [varchar](30) NOT NULL
                        )"""

create_dim = """delete  FROM [dbo].[DIM_Dataset] WHERE 
                Dataset_ID in (select Dataset_ID from [dbo].[DIM_Dataset_stage]
                        )"""
create_fact = """delete  FROM [dbo].[FACT_RefreshHistory] WHERE 
                request_id in (select request_id from [dbo].[FACT_RefreshHistory_stage])
                         """

insert_dim = """INSERT INTO DIM_Dataset(
                    Dataset_ID,
                    Dataset_Name,
                    Created_date,
                    [Description],
                    Dataset_owner
                    )
                    select * from DIM_Dataset_stage"""

insert_fact = """INSERT INTO FACT_RefreshHistory(
                    Dataset_ID,
                    Request_id,
                    RefreshType,
                    startTime,
                    endTime,
                    status,
                    Duration,
                    milestone
                    )
                    select Dataset_ID,
                    request_id,
                    refreshType,
                    startTime,
                    endTime,
                    status,
                    datediff(minute,starttime,endtime),
                    milestone
                    from FACT_RefreshHistory_stage
                    """

drop_stage_fact = """DROP TABLE IF EXISTS [dbo].[FACT_RefreshHistory_stage]"""
drop_stage_dim = """DROP TABLE IF EXISTS [dbo].[DIM_Dataset_stage] """

create_stage = [create_staging_dim,create_staging_fact]
create_dims = [create_dim]
create_facts = [create_fact]
insert_dims = [insert_dim]
insert_facts= [insert_fact]
drop_staging_tables = [drop_stage_dim,drop_stage_fact]