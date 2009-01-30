-- Add the order index to the dimension elements to preserve order.
ALTER TABLE XCUBE_DIMENSION_ELEMENTS ADD order_index int not null default (0);

-- dbid should be the primary key...
ALTER TABLE XCUBE_DIMENSION_ELEMENTS ADD [dbid] [int] IDENTITY(1,1) NOT NULL;
