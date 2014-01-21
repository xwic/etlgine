
/****** Object:  Table [dbo].[XCUBE_MEASURES]    Script Date: 12/29/2008 20:22:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[XCUBE_MEASURES] (
	[Key] [varchar](255) NOT NULL,
	[Title] [varchar](255) NULL,
	[FunctionClass] [varchar](300) NULL,
	[ValueFormatProvider] [varchar](300) NULL,
  CONSTRAINT [PK_XCUBE_MEASURES] PRIMARY KEY CLUSTERED 
(
	[Key] ASC
)) ON [PRIMARY]


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
	[Key] [varchar](255) NOT NULL,
	[Title] [varchar](255) NULL,
 CONSTRAINT [PK_XCUBE_DIMENSIONS] PRIMARY KEY CLUSTERED 
(
	[Key] ASC
)) ON [PRIMARY]

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
	[dbid] [int] IDENTITY(1,1) NOT NULL,
	[ID] [varchar](900) NOT NULL,
	[ParentID] [varchar](900) NOT NULL,
	[DimensionKey] [varchar](255) NOT NULL,
	[Key] [varchar](255) NOT NULL,
	[Title] [varchar](255) NULL,
	[weight] [float] NOT NULL,
	[order_index] [int] NOT NULL DEFAULT ((0)),
	 CONSTRAINT [PK_XCUBE_DIMENSION_ELEMENTS] PRIMARY KEY CLUSTERED 
(
	[dbid] ASC
)
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF

/****** Object:  Table [dbo].[XCUBE_DIMMAP]    Script Date: 01/07/2009 16:50:38 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[XCUBE_DIMMAP](
	[DimMapKey] [varchar](255) NOT NULL,
	[Description] [text] NULL,
	[DimensionKey] [varchar](255) NOT NULL,
	[UnmappedPath] [varchar](900) NULL,
	[OnUnmapped] [varchar](50) NOT NULL CONSTRAINT [DF_XCUBE_DIMMAP_OnUnmapped]  DEFAULT ('CREATE'),
 CONSTRAINT [PK_XCUBE_DIMMAP] PRIMARY KEY CLUSTERED 
(
	[DimMapKey] ASC
) ON [PRIMARY]
)

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[XCUBE_DIMMAP_ELEMENTS]    Script Date: 01/07/2009 16:50:38 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[XCUBE_DIMMAP_ELEMENTS](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DimMapKey] [varchar](255) NOT NULL,
	[Expression] [text]  NOT NULL,
	[isRegExp] [bit] NOT NULL,
	[IgnoreCase] [bit] NOT NULL,
	[ElementPath] [varchar](900) NULL,
	[SkipRecord] [bit] NOT NULL,
 CONSTRAINT [PK_XCUBE_DIMMAP_ELEMENTS] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
) ON [PRIMARY]
)

GO
SET ANSI_PADDING OFF