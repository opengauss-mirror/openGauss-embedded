/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     EQUALS_GREATER = 269,
     POWER_OF = 270,
     LAMBDA_ARROW = 271,
     DOUBLE_ARROW = 272,
     LESS_EQUALS = 273,
     GREATER_EQUALS = 274,
     NOT_EQUALS = 275,
     EQUALS = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANTI = 292,
     ANY = 293,
     ARRAY = 294,
     AS = 295,
     ASC_P = 296,
     ASSERTION = 297,
     ASSIGNMENT = 298,
     ASYMMETRIC = 299,
     AT = 300,
     ATTACH = 301,
     ATTRIBUTE = 302,
     AUTHORIZATION = 303,
     AUTOINCREMENT = 304,
     AUTOPART = 305,
     BACKWARD = 306,
     BEFORE = 307,
     BEGIN_P = 308,
     BETWEEN = 309,
     BIGINT = 310,
     BINARY = 311,
     BIT = 312,
     BOOLEAN_P = 313,
     BOTH = 314,
     BUCKET_WIDTH = 315,
     BY = 316,
     CACHE = 317,
     CAGGPOLICY = 318,
     CALL_P = 319,
     CALLED = 320,
     CASCADE = 321,
     CASCADED = 322,
     CASE = 323,
     CAST = 324,
     CATALOG_P = 325,
     CHAIN = 326,
     CHAR_P = 327,
     CHARACTER = 328,
     CHARACTERISTICS = 329,
     CHECK_P = 330,
     CHECKPOINT = 331,
     CLASS = 332,
     CLOSE = 333,
     CLUSTER = 334,
     COALESCE = 335,
     COLLATE = 336,
     COLLATION = 337,
     COLUMN = 338,
     COLUMNS = 339,
     COMMENT = 340,
     COMMENTS = 341,
     COMMIT = 342,
     COMMITTED = 343,
     COMPRESSION = 344,
     CONCURRENTLY = 345,
     CONFIGURATION = 346,
     CONFLICT = 347,
     CONNECTION = 348,
     CONSTRAINT = 349,
     CONSTRAINTS = 350,
     CONTENT_P = 351,
     CONTINUE_P = 352,
     CONVERSION_P = 353,
     COPY = 354,
     COST = 355,
     CREATE_P = 356,
     CROSS = 357,
     CROSSPART = 358,
     CSV = 359,
     CUBE = 360,
     CURRENT_P = 361,
     CURRENT_CATALOG = 362,
     CURRENT_DATE = 363,
     CURRENT_ROLE = 364,
     CURRENT_SCHEMA = 365,
     CURRENT_TIME = 366,
     CURRENT_TIMESTAMP = 367,
     CURRENT_USER = 368,
     CURSOR = 369,
     CYCLE = 370,
     DATA_P = 371,
     DATABASE = 372,
     DAY_P = 373,
     DAYS_P = 374,
     DEALLOCATE = 375,
     DEC = 376,
     DECIMAL_P = 377,
     DECLARE = 378,
     DEFAULT = 379,
     DEFAULTS = 380,
     DEFERRABLE = 381,
     DEFERRED = 382,
     DEFINER = 383,
     DELETE_P = 384,
     DELIMITER = 385,
     DELIMITERS = 386,
     DEPENDS = 387,
     DESC_P = 388,
     DESCRIBE = 389,
     DETACH = 390,
     DICTIONARY = 391,
     DISABLE_P = 392,
     DISCARD = 393,
     DISTINCT = 394,
     DO = 395,
     DOCUMENT_P = 396,
     DOMAIN_P = 397,
     DOUBLE_P = 398,
     DROP = 399,
     EACH = 400,
     ELSE = 401,
     ENABLE_P = 402,
     ENCODING = 403,
     ENCRYPTED = 404,
     END_P = 405,
     ENUM_P = 406,
     ESCAPE = 407,
     EVENT = 408,
     EXCEPT = 409,
     EXCLUDE = 410,
     EXCLUDING = 411,
     EXCLUSIVE = 412,
     EXECUTE = 413,
     EXISTS = 414,
     EXPLAIN = 415,
     EXPORT_P = 416,
     EXPORT_STATE = 417,
     EXTENSION = 418,
     EXTERNAL = 419,
     EXTRACT = 420,
     FALSE_P = 421,
     FAMILY = 422,
     FETCH = 423,
     FILTER = 424,
     FIRST_P = 425,
     FLOAT_P = 426,
     FOLLOWING = 427,
     FOR = 428,
     FORCE = 429,
     FOREIGN = 430,
     FORWARD = 431,
     FREEZE = 432,
     FROM = 433,
     FULL = 434,
     FUNCTION = 435,
     FUNCTIONS = 436,
     GENERATED = 437,
     GLOB = 438,
     GLOBAL = 439,
     GRANT = 440,
     GRANTED = 441,
     GROUP_P = 442,
     GROUPING = 443,
     GROUPING_ID = 444,
     HANDLER = 445,
     HAVING = 446,
     HEADER_P = 447,
     HOLD = 448,
     HOUR_P = 449,
     HOURS_P = 450,
     IDENTITY_P = 451,
     IF_P = 452,
     IGNORE_P = 453,
     ILIKE = 454,
     IMMEDIATE = 455,
     IMMUTABLE = 456,
     IMPLICIT_P = 457,
     IMPORT_P = 458,
     IN_P = 459,
     INCLUDE_P = 460,
     INCLUDING = 461,
     INCREMENT = 462,
     INDEX = 463,
     INDEXES = 464,
     INHERIT = 465,
     INHERITS = 466,
     INITIALLY = 467,
     INITTIME_P = 468,
     INLINE_P = 469,
     INNER_P = 470,
     INOUT = 471,
     INPUT_P = 472,
     INSENSITIVE = 473,
     INSERT = 474,
     INSTALL = 475,
     INSTEAD = 476,
     INT_P = 477,
     INTEGER = 478,
     INTERSECT = 479,
     INTERVAL = 480,
     INTO = 481,
     INVOKER = 482,
     IS = 483,
     ISNULL = 484,
     ISOLATION = 485,
     JOIN = 486,
     JSON = 487,
     KEY = 488,
     LABEL = 489,
     LANGUAGE = 490,
     LARGE_P = 491,
     LAST_P = 492,
     LATERAL_P = 493,
     LEADING = 494,
     LEAKPROOF = 495,
     LEFT = 496,
     LEVEL = 497,
     LIKE = 498,
     LIMIT = 499,
     LISTEN = 500,
     LOAD = 501,
     LOCAL = 502,
     LOCALTIME = 503,
     LOCALTIMESTAMP = 504,
     LOCATION = 505,
     LOCK_P = 506,
     LOCKED = 507,
     LOGGED = 508,
     MACRO = 509,
     MAP = 510,
     MAPPING = 511,
     MATCH = 512,
     MATERIALIZED = 513,
     MAXVALUE = 514,
     METHOD = 515,
     MICROSECOND_P = 516,
     MICROSECONDS_P = 517,
     MILLISECOND_P = 518,
     MILLISECONDS_P = 519,
     MINUTE_P = 520,
     MINUTES_P = 521,
     MINVALUE = 522,
     MODE = 523,
     MONTH_P = 524,
     MONTHS_P = 525,
     MOVE = 526,
     NAME_P = 527,
     NAMES = 528,
     NATIONAL = 529,
     NATURAL = 530,
     NCHAR = 531,
     NEW = 532,
     NEXT = 533,
     NO = 534,
     NONE = 535,
     NOT = 536,
     NOTHING = 537,
     NOTIFY = 538,
     NOTNULL = 539,
     NOWAIT = 540,
     NULL_P = 541,
     NULLIF = 542,
     NULLS_P = 543,
     NUMERIC = 544,
     OBJECT_P = 545,
     OF = 546,
     OFF = 547,
     OFFSET = 548,
     OIDS = 549,
     OLD = 550,
     ON = 551,
     ONLY = 552,
     OPERATOR = 553,
     OPTION = 554,
     OPTIONS = 555,
     OR = 556,
     ORDER = 557,
     ORDINALITY = 558,
     OUT_P = 559,
     OUTER_P = 560,
     OVER = 561,
     OVERLAPS = 562,
     OVERLAY = 563,
     OVERRIDING = 564,
     OWNED = 565,
     OWNER = 566,
     PARALLEL = 567,
     PARSER = 568,
     PARTIAL = 569,
     PARTITION = 570,
     PASSING = 571,
     PASSWORD = 572,
     PERCENT = 573,
     PIVOT = 574,
     PIVOT_LONGER = 575,
     PIVOT_WIDER = 576,
     PLACING = 577,
     PLANS = 578,
     POLICY = 579,
     POSITION = 580,
     POSITIONAL = 581,
     PRAGMA_P = 582,
     PRECEDING = 583,
     PRECISION = 584,
     PREPARE = 585,
     PREPARED = 586,
     PRESERVE = 587,
     PRIMARY = 588,
     PRIOR = 589,
     PRIVILEGES = 590,
     PROCEDURAL = 591,
     PROCEDURE = 592,
     PROGRAM = 593,
     PUBLICATION = 594,
     QUALIFY = 595,
     QUARTER_P = 596,
     QUARTERS_P = 597,
     QUOTE = 598,
     RANGE = 599,
     READ_P = 600,
     REAL = 601,
     REASSIGN = 602,
     RECHECK = 603,
     RECURSIVE = 604,
     REF = 605,
     REFERENCES = 606,
     REFERENCING = 607,
     REFREASH_INTERVAL = 608,
     REFRESH = 609,
     REINDEX = 610,
     RELATIVE_P = 611,
     RELEASE = 612,
     RENAME = 613,
     REPEATABLE = 614,
     REPLACE = 615,
     REPLICA = 616,
     RESET = 617,
     RESPECT_P = 618,
     RESTART = 619,
     RESTRICT = 620,
     RETENTION = 621,
     RETURNING = 622,
     RETURNS = 623,
     REVOKE = 624,
     RIGHT = 625,
     ROLE = 626,
     ROLLBACK = 627,
     ROLLUP = 628,
     ROW = 629,
     ROWS = 630,
     RULE = 631,
     SAMPLE = 632,
     SAVEPOINT = 633,
     SCHEMA = 634,
     SCHEMAS = 635,
     SCROLL = 636,
     SEARCH = 637,
     SECOND_P = 638,
     SECONDS_P = 639,
     SECURITY = 640,
     SELECT = 641,
     SEMI = 642,
     SEQUENCE = 643,
     SEQUENCES = 644,
     SERIALIZABLE = 645,
     SERVER = 646,
     SESSION = 647,
     SESSION_USER = 648,
     SET = 649,
     SETOF = 650,
     SETS = 651,
     SHARE = 652,
     SHOW = 653,
     SIMILAR = 654,
     SIMPLE = 655,
     SKIP = 656,
     SMALLINT = 657,
     SNAPSHOT = 658,
     SOME = 659,
     SQL_P = 660,
     STABLE = 661,
     STANDALONE_P = 662,
     START = 663,
     STATEMENT = 664,
     STATISTICS = 665,
     STDIN = 666,
     STDOUT = 667,
     STORAGE = 668,
     STORED = 669,
     STRICT_P = 670,
     STRIP_P = 671,
     STRUCT = 672,
     SUBSCRIPTION = 673,
     SUBSTRING = 674,
     SUMMARIZE = 675,
     SYMMETRIC = 676,
     SYNONYM = 677,
     SYSID = 678,
     SYSTEM_P = 679,
     TABLE = 680,
     TABLES = 681,
     TABLESAMPLE = 682,
     TABLESPACE = 683,
     TEMP = 684,
     TEMPLATE = 685,
     TEMPORARY = 686,
     TEXT_P = 687,
     THEN = 688,
     TIME = 689,
     TIMESCALE = 690,
     TIMESTAMP = 691,
     TO = 692,
     TRAILING = 693,
     TRANSACTION = 694,
     TRANSFORM = 695,
     TREAT = 696,
     TRIGGER = 697,
     TRIM = 698,
     TRUE_P = 699,
     TRUNCATE = 700,
     TRUSTED = 701,
     TRY_CAST = 702,
     TYPE_P = 703,
     TYPES_P = 704,
     UNBOUNDED = 705,
     UNCOMMITTED = 706,
     UNENCRYPTED = 707,
     UNION = 708,
     UNIQUE = 709,
     UNKNOWN = 710,
     UNLISTEN = 711,
     UNLOGGED = 712,
     UNPIVOT = 713,
     UNTIL = 714,
     UPDATE = 715,
     USE_P = 716,
     USER = 717,
     USERS = 718,
     USING = 719,
     VACUUM = 720,
     VALID = 721,
     VALIDATE = 722,
     VALIDATOR = 723,
     VALUE_P = 724,
     VALUES = 725,
     VARCHAR = 726,
     VARIABLES_P = 727,
     VARIADIC = 728,
     VARYING = 729,
     VERBOSE = 730,
     VERSION_P = 731,
     VIEW = 732,
     VIEWS = 733,
     VIRTUAL = 734,
     VOLATILE = 735,
     WAIT_P = 736,
     WEEK_P = 737,
     WEEKS_P = 738,
     WHEN = 739,
     WHERE = 740,
     WHITESPACE_P = 741,
     WINDOW = 742,
     WITH = 743,
     WITHIN = 744,
     WITHOUT = 745,
     WORK = 746,
     WRAPPER = 747,
     WRITE_P = 748,
     XML_P = 749,
     XMLATTRIBUTES = 750,
     XMLCONCAT = 751,
     XMLELEMENT = 752,
     XMLEXISTS = 753,
     XMLFOREST = 754,
     XMLNAMESPACES = 755,
     XMLPARSE = 756,
     XMLPI = 757,
     XMLROOT = 758,
     XMLSERIALIZE = 759,
     XMLTABLE = 760,
     YEAR_P = 761,
     YEARS_P = 762,
     YES_P = 763,
     ZONE = 764,
     NOT_LA = 765,
     NULLS_LA = 766,
     WITH_LA = 767,
     POSTFIXOP = 768,
     UMINUS = 769
   };
#endif
/* Tokens.  */
#define IDENT 258
#define FCONST 259
#define SCONST 260
#define BCONST 261
#define XCONST 262
#define Op 263
#define ICONST 264
#define PARAM 265
#define TYPECAST 266
#define DOT_DOT 267
#define COLON_EQUALS 268
#define EQUALS_GREATER 269
#define POWER_OF 270
#define LAMBDA_ARROW 271
#define DOUBLE_ARROW 272
#define LESS_EQUALS 273
#define GREATER_EQUALS 274
#define NOT_EQUALS 275
#define EQUALS 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANTI 292
#define ANY 293
#define ARRAY 294
#define AS 295
#define ASC_P 296
#define ASSERTION 297
#define ASSIGNMENT 298
#define ASYMMETRIC 299
#define AT 300
#define ATTACH 301
#define ATTRIBUTE 302
#define AUTHORIZATION 303
#define AUTOINCREMENT 304
#define AUTOPART 305
#define BACKWARD 306
#define BEFORE 307
#define BEGIN_P 308
#define BETWEEN 309
#define BIGINT 310
#define BINARY 311
#define BIT 312
#define BOOLEAN_P 313
#define BOTH 314
#define BUCKET_WIDTH 315
#define BY 316
#define CACHE 317
#define CAGGPOLICY 318
#define CALL_P 319
#define CALLED 320
#define CASCADE 321
#define CASCADED 322
#define CASE 323
#define CAST 324
#define CATALOG_P 325
#define CHAIN 326
#define CHAR_P 327
#define CHARACTER 328
#define CHARACTERISTICS 329
#define CHECK_P 330
#define CHECKPOINT 331
#define CLASS 332
#define CLOSE 333
#define CLUSTER 334
#define COALESCE 335
#define COLLATE 336
#define COLLATION 337
#define COLUMN 338
#define COLUMNS 339
#define COMMENT 340
#define COMMENTS 341
#define COMMIT 342
#define COMMITTED 343
#define COMPRESSION 344
#define CONCURRENTLY 345
#define CONFIGURATION 346
#define CONFLICT 347
#define CONNECTION 348
#define CONSTRAINT 349
#define CONSTRAINTS 350
#define CONTENT_P 351
#define CONTINUE_P 352
#define CONVERSION_P 353
#define COPY 354
#define COST 355
#define CREATE_P 356
#define CROSS 357
#define CROSSPART 358
#define CSV 359
#define CUBE 360
#define CURRENT_P 361
#define CURRENT_CATALOG 362
#define CURRENT_DATE 363
#define CURRENT_ROLE 364
#define CURRENT_SCHEMA 365
#define CURRENT_TIME 366
#define CURRENT_TIMESTAMP 367
#define CURRENT_USER 368
#define CURSOR 369
#define CYCLE 370
#define DATA_P 371
#define DATABASE 372
#define DAY_P 373
#define DAYS_P 374
#define DEALLOCATE 375
#define DEC 376
#define DECIMAL_P 377
#define DECLARE 378
#define DEFAULT 379
#define DEFAULTS 380
#define DEFERRABLE 381
#define DEFERRED 382
#define DEFINER 383
#define DELETE_P 384
#define DELIMITER 385
#define DELIMITERS 386
#define DEPENDS 387
#define DESC_P 388
#define DESCRIBE 389
#define DETACH 390
#define DICTIONARY 391
#define DISABLE_P 392
#define DISCARD 393
#define DISTINCT 394
#define DO 395
#define DOCUMENT_P 396
#define DOMAIN_P 397
#define DOUBLE_P 398
#define DROP 399
#define EACH 400
#define ELSE 401
#define ENABLE_P 402
#define ENCODING 403
#define ENCRYPTED 404
#define END_P 405
#define ENUM_P 406
#define ESCAPE 407
#define EVENT 408
#define EXCEPT 409
#define EXCLUDE 410
#define EXCLUDING 411
#define EXCLUSIVE 412
#define EXECUTE 413
#define EXISTS 414
#define EXPLAIN 415
#define EXPORT_P 416
#define EXPORT_STATE 417
#define EXTENSION 418
#define EXTERNAL 419
#define EXTRACT 420
#define FALSE_P 421
#define FAMILY 422
#define FETCH 423
#define FILTER 424
#define FIRST_P 425
#define FLOAT_P 426
#define FOLLOWING 427
#define FOR 428
#define FORCE 429
#define FOREIGN 430
#define FORWARD 431
#define FREEZE 432
#define FROM 433
#define FULL 434
#define FUNCTION 435
#define FUNCTIONS 436
#define GENERATED 437
#define GLOB 438
#define GLOBAL 439
#define GRANT 440
#define GRANTED 441
#define GROUP_P 442
#define GROUPING 443
#define GROUPING_ID 444
#define HANDLER 445
#define HAVING 446
#define HEADER_P 447
#define HOLD 448
#define HOUR_P 449
#define HOURS_P 450
#define IDENTITY_P 451
#define IF_P 452
#define IGNORE_P 453
#define ILIKE 454
#define IMMEDIATE 455
#define IMMUTABLE 456
#define IMPLICIT_P 457
#define IMPORT_P 458
#define IN_P 459
#define INCLUDE_P 460
#define INCLUDING 461
#define INCREMENT 462
#define INDEX 463
#define INDEXES 464
#define INHERIT 465
#define INHERITS 466
#define INITIALLY 467
#define INITTIME_P 468
#define INLINE_P 469
#define INNER_P 470
#define INOUT 471
#define INPUT_P 472
#define INSENSITIVE 473
#define INSERT 474
#define INSTALL 475
#define INSTEAD 476
#define INT_P 477
#define INTEGER 478
#define INTERSECT 479
#define INTERVAL 480
#define INTO 481
#define INVOKER 482
#define IS 483
#define ISNULL 484
#define ISOLATION 485
#define JOIN 486
#define JSON 487
#define KEY 488
#define LABEL 489
#define LANGUAGE 490
#define LARGE_P 491
#define LAST_P 492
#define LATERAL_P 493
#define LEADING 494
#define LEAKPROOF 495
#define LEFT 496
#define LEVEL 497
#define LIKE 498
#define LIMIT 499
#define LISTEN 500
#define LOAD 501
#define LOCAL 502
#define LOCALTIME 503
#define LOCALTIMESTAMP 504
#define LOCATION 505
#define LOCK_P 506
#define LOCKED 507
#define LOGGED 508
#define MACRO 509
#define MAP 510
#define MAPPING 511
#define MATCH 512
#define MATERIALIZED 513
#define MAXVALUE 514
#define METHOD 515
#define MICROSECOND_P 516
#define MICROSECONDS_P 517
#define MILLISECOND_P 518
#define MILLISECONDS_P 519
#define MINUTE_P 520
#define MINUTES_P 521
#define MINVALUE 522
#define MODE 523
#define MONTH_P 524
#define MONTHS_P 525
#define MOVE 526
#define NAME_P 527
#define NAMES 528
#define NATIONAL 529
#define NATURAL 530
#define NCHAR 531
#define NEW 532
#define NEXT 533
#define NO 534
#define NONE 535
#define NOT 536
#define NOTHING 537
#define NOTIFY 538
#define NOTNULL 539
#define NOWAIT 540
#define NULL_P 541
#define NULLIF 542
#define NULLS_P 543
#define NUMERIC 544
#define OBJECT_P 545
#define OF 546
#define OFF 547
#define OFFSET 548
#define OIDS 549
#define OLD 550
#define ON 551
#define ONLY 552
#define OPERATOR 553
#define OPTION 554
#define OPTIONS 555
#define OR 556
#define ORDER 557
#define ORDINALITY 558
#define OUT_P 559
#define OUTER_P 560
#define OVER 561
#define OVERLAPS 562
#define OVERLAY 563
#define OVERRIDING 564
#define OWNED 565
#define OWNER 566
#define PARALLEL 567
#define PARSER 568
#define PARTIAL 569
#define PARTITION 570
#define PASSING 571
#define PASSWORD 572
#define PERCENT 573
#define PIVOT 574
#define PIVOT_LONGER 575
#define PIVOT_WIDER 576
#define PLACING 577
#define PLANS 578
#define POLICY 579
#define POSITION 580
#define POSITIONAL 581
#define PRAGMA_P 582
#define PRECEDING 583
#define PRECISION 584
#define PREPARE 585
#define PREPARED 586
#define PRESERVE 587
#define PRIMARY 588
#define PRIOR 589
#define PRIVILEGES 590
#define PROCEDURAL 591
#define PROCEDURE 592
#define PROGRAM 593
#define PUBLICATION 594
#define QUALIFY 595
#define QUARTER_P 596
#define QUARTERS_P 597
#define QUOTE 598
#define RANGE 599
#define READ_P 600
#define REAL 601
#define REASSIGN 602
#define RECHECK 603
#define RECURSIVE 604
#define REF 605
#define REFERENCES 606
#define REFERENCING 607
#define REFREASH_INTERVAL 608
#define REFRESH 609
#define REINDEX 610
#define RELATIVE_P 611
#define RELEASE 612
#define RENAME 613
#define REPEATABLE 614
#define REPLACE 615
#define REPLICA 616
#define RESET 617
#define RESPECT_P 618
#define RESTART 619
#define RESTRICT 620
#define RETENTION 621
#define RETURNING 622
#define RETURNS 623
#define REVOKE 624
#define RIGHT 625
#define ROLE 626
#define ROLLBACK 627
#define ROLLUP 628
#define ROW 629
#define ROWS 630
#define RULE 631
#define SAMPLE 632
#define SAVEPOINT 633
#define SCHEMA 634
#define SCHEMAS 635
#define SCROLL 636
#define SEARCH 637
#define SECOND_P 638
#define SECONDS_P 639
#define SECURITY 640
#define SELECT 641
#define SEMI 642
#define SEQUENCE 643
#define SEQUENCES 644
#define SERIALIZABLE 645
#define SERVER 646
#define SESSION 647
#define SESSION_USER 648
#define SET 649
#define SETOF 650
#define SETS 651
#define SHARE 652
#define SHOW 653
#define SIMILAR 654
#define SIMPLE 655
#define SKIP 656
#define SMALLINT 657
#define SNAPSHOT 658
#define SOME 659
#define SQL_P 660
#define STABLE 661
#define STANDALONE_P 662
#define START 663
#define STATEMENT 664
#define STATISTICS 665
#define STDIN 666
#define STDOUT 667
#define STORAGE 668
#define STORED 669
#define STRICT_P 670
#define STRIP_P 671
#define STRUCT 672
#define SUBSCRIPTION 673
#define SUBSTRING 674
#define SUMMARIZE 675
#define SYMMETRIC 676
#define SYNONYM 677
#define SYSID 678
#define SYSTEM_P 679
#define TABLE 680
#define TABLES 681
#define TABLESAMPLE 682
#define TABLESPACE 683
#define TEMP 684
#define TEMPLATE 685
#define TEMPORARY 686
#define TEXT_P 687
#define THEN 688
#define TIME 689
#define TIMESCALE 690
#define TIMESTAMP 691
#define TO 692
#define TRAILING 693
#define TRANSACTION 694
#define TRANSFORM 695
#define TREAT 696
#define TRIGGER 697
#define TRIM 698
#define TRUE_P 699
#define TRUNCATE 700
#define TRUSTED 701
#define TRY_CAST 702
#define TYPE_P 703
#define TYPES_P 704
#define UNBOUNDED 705
#define UNCOMMITTED 706
#define UNENCRYPTED 707
#define UNION 708
#define UNIQUE 709
#define UNKNOWN 710
#define UNLISTEN 711
#define UNLOGGED 712
#define UNPIVOT 713
#define UNTIL 714
#define UPDATE 715
#define USE_P 716
#define USER 717
#define USERS 718
#define USING 719
#define VACUUM 720
#define VALID 721
#define VALIDATE 722
#define VALIDATOR 723
#define VALUE_P 724
#define VALUES 725
#define VARCHAR 726
#define VARIABLES_P 727
#define VARIADIC 728
#define VARYING 729
#define VERBOSE 730
#define VERSION_P 731
#define VIEW 732
#define VIEWS 733
#define VIRTUAL 734
#define VOLATILE 735
#define WAIT_P 736
#define WEEK_P 737
#define WEEKS_P 738
#define WHEN 739
#define WHERE 740
#define WHITESPACE_P 741
#define WINDOW 742
#define WITH 743
#define WITHIN 744
#define WITHOUT 745
#define WORK 746
#define WRAPPER 747
#define WRITE_P 748
#define XML_P 749
#define XMLATTRIBUTES 750
#define XMLCONCAT 751
#define XMLELEMENT 752
#define XMLEXISTS 753
#define XMLFOREST 754
#define XMLNAMESPACES 755
#define XMLPARSE 756
#define XMLPI 757
#define XMLROOT 758
#define XMLSERIALIZE 759
#define XMLTABLE 760
#define YEAR_P 761
#define YEARS_P 762
#define YES_P 763
#define ZONE 764
#define NOT_LA 765
#define NULLS_LA 766
#define WITH_LA 767
#define POSTFIXOP 768
#define UMINUS 769




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 14 "dependency/libpg_query/grammar/grammar.y"
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
    PGPartitionElem *partelem;
	PGPartitionSpec *partspec;
	PGPartitionBoundSpec *partboundspec;
	PGRoleSpec   *rolespec;
	PGPrivTarget *privtarget;
	PGAccessPriv *accesspriv;
}
/* Line 1489 of yacc.c.  */
#line 1129 "dependency/libpg_query/grammar/grammar_out.hpp"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


