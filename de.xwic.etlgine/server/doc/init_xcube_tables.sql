USE [etlgine_test]
GO
/****** Object:  Table [dbo].[XCUBE_MEASURES]    Script Date: 12/29/2008 20:22:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[XCUBE_MEASURES](
	[Key] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[Title] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
 CONSTRAINT [PK_XCUBE_MEASURES] PRIMARY KEY CLUSTERED 
(
	[Key] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[XCUBE_DIMENSIONS]    Script Date: 12/29/2008 20:22:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[XCUBE_DIMENSIONS](
	[Key] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[Title] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
 CONSTRAINT [PK_XCUBE_DIMENSIONS] PRIMARY KEY CLUSTERED 
(
	[Key] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[XCUBE_DIMENSION_ELEMENTS]    Script Date: 12/29/2008 20:22:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[XCUBE_DIMENSION_ELEMENTS](
	[ID] [varchar](900) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[ParentID] [varchar](900) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[DimensionKey] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[Key] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[Title] [varchar](255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[weight] [float] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF