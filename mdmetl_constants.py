# mdmetl_constants

TEMP_GIFT_CREATE_TABLE = '''\
    create table Temp.{tablename}
(
    Client   varchar(10)    not null,
    GiftID       varchar(255),
    DonorID      varchar(255)   not null,
    GiftDate     date           not null,
    GiftAmount   numeric(18, 2) not null,
    GiftType     varchar(255),
    GiftType2    varchar(255),
    PaymentType  varchar(255),
    Appeal       varchar(255),
    AppealDesc   varchar(255),
    Package      varchar(255),
    PackageDesc  varchar(255),
    Campaign     varchar(255),
    CampaignDesc varchar(255),
    Fund         varchar(255),
    FundDesc     varchar(255),
    Channel      varchar(255),
    IsPledge     varchar(255),
    Anonymous    varchar(255)
)
'''




