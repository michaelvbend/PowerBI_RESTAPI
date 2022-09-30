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
                            [Dataset_owner] [varchar](30) NOT NULL,
                            [Workspace_ID] varchar(50) NOT NULL
                        )"""

create_staging_dim_workspace = """DROP TABLE IF EXISTS [dbo].[DIM_Workspace_stage] 
                         CREATE TABLE [dbo].[DIM_Workspace_stage](
                            [Workspace_ID] [varchar](50) NOT NULL,
                            [Workspace_Name] [varchar](50) NOT NULL
                        )"""

create_dim = """delete  FROM [dbo].[DIM_Dataset] WHERE 
                concat(Dataset_ID,workspace_ID) in (select concat(Dataset_ID,workspace_id) from [dbo].[DIM_Dataset_stage]
                        )"""

create_fact = """delete  FROM [dbo].[FACT_RefreshHistory] WHERE 
                request_id in (select request_id from [dbo].[FACT_RefreshHistory_stage])
                         """
create_dim_workspace = """delete  FROM [dbo].[DIM_Workspace] WHERE 
                workspace_ID in (select workspace_ID from [dbo].[DIM_Workspace_stage])
                         """

insert_dim = """INSERT INTO DIM_Dataset(
                    Dataset_ID,
                    Dataset_Name,
                    Created_date,
                    [Description],
                    Dataset_owner,
                    Workspace_ID
                    )
                    select * from DIM_Dataset_stage where concat(dataset_id,workspace_ID) not in (select concat(dataset_id,workspace_ID) from DIM_Dataset)"""

insert_dim_workspace = """INSERT INTO DIM_Workspace(
                    Workspace_ID,
                    Workspace_Name
                    )
                    select * from DIM_Workspace_stage where concat(workspace_ID,workspace_name) not in (select concat(workspace_ID,workspace_name) from DIM_Workspace)"""

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
drop_stage_dim_workspace = """DROP TABLE IF EXISTS [dbo].[DIM_Workspace_stage] """


create_stage = [create_staging_dim,create_staging_fact]
create_dims = [create_dim]
create_facts = [create_fact]
insert_dims = [insert_dim]
insert_facts= [insert_fact]
drop_staging_tables = [drop_stage_dim,drop_stage_fact]