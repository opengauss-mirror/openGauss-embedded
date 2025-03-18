/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cm_dec8.c
 * This file high-precision dec8. The number 8 means each digital cell
 * can capacity accommodate 8 digits. Here, the digital cells are defied by UINT32.
 * dec8 is more efficient than dec4, and thus it is designed for numeric computing.
 * The performance test can be found in the project zperf_test.
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/cm_dec8.c
 *
 * -------------------------------------------------------------------------
 */
#include "cm_text.h"
#include "cm_decimal.h"
#include "cm_binary.h"

#ifdef __cplusplus
extern "C" {
#endif


/* DEC8_POW2_MASK is 10^8 */
#define DEC8_POW2_MASK ((uint64)(DEC8_CELL_MASK) * (DEC8_CELL_MASK))


static const uint64 g_pow8_u64[] = {
    1,              // 10^0
    DEC8_CELL_MASK, // 10^8
    DEC8_POW2_MASK, // 10^16
};

/* * The following define some useful constants */
/* decimal 0 */
static const dec8_t DEC8_ZERO = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = 0,
    .cells = { 0 }
};

/* decimal 0.5 */
static const dec8_t DEC8_HALF_ONE = {
    .expn = SEXP_2_D8EXP(-DEC8_CELL_DIGIT),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)1,
    .cells = { 50000000 }
};

/* decimal 1 */
const dec8_t DEC8_ONE = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)1,
    .cells = { 1 }
};

/* decimal -1 */
static const dec8_t DEC8_NEG_ONE = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_MINUS,
    .ncells = (uint8)1,
    .cells = { 1 }
};

/* decimal 2 */
static const dec8_t DEC8_TWO = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)1,
    .cells = { 2 }
};

/* decimal 4 */
static const dec8_t DEC8_FOUR = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)1,
    .cells = { 4 }
};

/* decimal pi/2 is 1.570796326794896619231321691639751442098584699687552910487472296153908 */
static const dec8_t DEC8_HALF_PI = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = DEC8_CELL_SIZE,
    .cells = { 1, 57079632, 67948966, 19231321, 69163975, 14420985, 84699688 }
};

/* decimal pi is 3.1415926535897932384626433832795028841971693993751058209749445923078164 */
static const dec8_t DEC8_PI = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = DEC8_CELL_SIZE,
    .cells = { 3, 14159265, 35897932, 38462643, 38327950, 28841971, 69399375 }
};

/* decimal 2*pi is 6.28318530717958647692528676655900576839433879875021164194988918461563281 */
static const dec8_t DEC8_2PI = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = DEC8_CELL_SIZE,
    .cells = { 6, 28318530, 71795864, 76925286, 76655900, 57683943, 38798750 }
};

/* 1/(2pi) is 0.159154943091895335768883763372514362034459645740456448747667344058896797634226535 */
static const dec8_t DEC8_INV_2PI = {
    .expn = SEXP_2_D8EXP(-DEC8_CELL_DIGIT),
    .sign = DEC_SIGN_PLUS,
    .ncells = DEC8_CELL_SIZE,
    .cells = { 15915494, 30918953, 35768883, 76337251, 43620344, 59645740, 45644875 }
};

/* decimal of the minimal int64 is -9 223 372 036 854 775 808 */
const dec8_t DEC8_MIN_INT64 = {
    // to make the expn be the integer multiple times of DEC_CELL_DIGIT
    .expn = SEXP_2_D8EXP(16),
    .sign = DEC_SIGN_MINUS,
    .ncells = (uint8)3,
    .cells = { 922, 33720368, 54775808 }
};

/* decimal of the maximal bigint is 9,223,372,036,854,775,807 */
const dec8_t DEC8_MAX_INT64 = {
    .expn = SEXP_2_D8EXP(16),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)3,
    .cells = { 922, 33720368, 54775807 }
};

/* decimal of the maximal uint64 is 18,446,744,073,709,551,615 */
const dec8_t DEC8_MAX_UINT64 = {
    .expn = SEXP_2_D8EXP(16),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)3,
    .cells = { 1844, 67440737, 9551615 }
};

/* decimal of the minimal int32 is -2 147 483 648 */
static const dec8_t DEC8_MIN_INT32 = {
    // to make the expn be the integer multiple times of DEC_CELL_DIGIT
    .expn = SEXP_2_D8EXP(8),
    .sign = DEC_SIGN_MINUS,
    .ncells = (uint8)2,
    .cells = { 21, 47483648 }
};

/* 2.71828182845904523536028747135266249775724709369995957496696762772407663 */
static const dec8_t DEC8_EXP = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = DEC8_CELL_SIZE,
    .cells = { 2, 71828182, 84590452, 35360287, 47135266, 24977572, 47093699 }
};

/* ln(10) is 2.3025850929940456840179914546843642076011014886287729760333279009675726 */
static const dec8_t DEC8_LN10 = {
    .expn = SEXP_2_D8EXP(0),
    .sign = DEC_SIGN_PLUS,
    .ncells = DEC8_CELL_SIZE,
    .cells = { 2, 30258509, 29940456, 84017991, 45468436, 42076011, 1488629 }
};

#define INV_FACT_START 3
#define _I(i) ((i)-INV_FACT_START)
static const dec8_t g_dec8_inv_fact[] = {
    /* 1/3! = 0.166666666666666666666666666666666666666666666666666666666666666666666666666 */
    [_I(3)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/3! */
        .cells = { 16666666, 66666666, 66666666, 66666666, 66666666, 66666666, 66666667 }
    },
    /* 1/4! =
     * 0.04166666666666666666666666666666666666666666666666666666666666666666666666666666666666666
     * 6666666666666666666666 */
    [_I(4)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/3! */
        .cells = { 4166666, 66666666, 66666666, 66666666, 66666666, 66666666, 66666667 }
    },
    /* 1/5! = 0.0083333333333333333333333333333333333333333333333333333333333333333333333333333 */
    [_I(5)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/5! */
        .cells = { 833333, 33333333, 33333333, 33333333, 33333333, 33333333, 33333333 }
    },
    /* 1/6! =
     * 0.001388888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888
     * 8888888888888888888888888888888888888888888888888888888
     */
    [_I(6)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/5! */
        .cells = { 138888, 88888888, 88888888, 88888888, 88888888, 88888888, 88888889 }
    },
    /* 1/7! = 0.0001984126984126984126984126984126984126984126984126984126984126984126984126984126984 */
    [_I(7)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/7! */
        .cells = { 19841, 26984126, 98412698, 41269841, 26984126, 98412698, 41269841 }
    },
    /* 1/8! =
     * 0.0000248015873015873015873015873015873015873015873015873015873015873015873015873015873015873
     * 015873015873015873015873015873015873015873015873015873015873015873
     */
    [_I(8)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/7! */
        .cells = { 2480, 15873015, 87301587, 30158730, 15873015, 87301587, 30158730 }
    },
    /* 1/9! = 0.00000275573192239858906525573192239858906525573192239858906525573192239858906525573192 */
    [_I(9)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE,
        .cells = { 275, 57319223, 98589065, 25573192, 23985890, 65255731, 92239859 }
    },
    /* 1/10! = 0.00000275573192239858906525573192239858906525573192239858906525573192239858906525573192 */
    [_I(10)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE,
        .cells = { 27, 55731922, 39858906, 52557319, 22398589, 6525573, 19223986 }
    },
    /* 1/11! =
     * 0.000000025052108385441718775052108385441718775052108385441718775052108385441718775
     * 0521083854417187750521083854417187750521
     */
    [_I(11)] = {
        .expn = SEXP_2_D8EXP(-8), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/11! */
        .cells = { 2, 50521083, 85441718, 77505210, 83854417, 18775052, 10838544 }
    },
    /* 1/12! =
     * 0.0000000020876756987868098979210090321201432312543423654534765645876756987868098979210090
     * 3212014323125434236545347656458767569878680989792
     */
    [_I(12)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/12! */
        .cells = { 20876756, 98786809, 89792100, 90321201, 43231254, 34236545, 34765646 }
    },
    /* 1/13! =
     * 0.00000000016059043836821614599392377170154947932725710503488281266059043836821614599392377
     * 170154947932725710503488281266
     */
    [_I(13)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/13! */
        .cells = { 1605904, 38368216, 14599392, 37717015, 49479327, 25710503, 48828127 }
    },
    /* 1/14! =
     * 0.0000000000114707455977297247138516979786821056662326503596344866186136027405868675709945551215
     * 3924852337550750249162947575645988344401042813741226439639138
     */
    [_I(14)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/14! */
        .cells = { 114707, 45597729, 72471385, 16979786, 82105666, 23265035, 96344867 }
    },
    /* 1/15! =
     * 0.0000000000007647163731819816475901131985788070444155100239756324412409068493724578380663036
     * 74769283234891700500166108631717
     */
    [_I(15)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/15! */
        .cells = { 7647, 16373181, 98164759, 1131985, 78807044, 41551002, 39756324 }
    },
    /* 1/16! =
     * 0.0000000000000477947733238738529743820749111754402759693764984770275775566780857786148791439
     * 79673080202180731281260381789482318582847683
     */
    [_I(16)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/16! */
        .cells = { 477, 94773323, 87385297, 43820749, 11175440, 27596937, 64984770 }
    },
    /* 1/17! =
     * 0.00000000000000281145725434552076319894558301032001623349273520453103397392224033991852230
     * 258703959295306945478125061069
     */
    [_I(17)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/17! */
        .cells = { 28, 11457254, 34552076, 31989455, 83010320, 1623349, 27352045 }
    },
    /* 1/18! =
     * 0.00000000000000015619206968586226462216364350057333423519404084469616855410679112999547346
     * 125483553294183719193229170059408327555092
     */
    [_I(18)] = {
        .expn = SEXP_2_D8EXP(-16), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/19! */
        .cells = { 1, 56192069, 68586226, 46221636, 43500573, 33423519, 40408447 }
    },
    /* 1/19! =
     * 0.00000000000000000822063524662432971695598123687228074922073899182611413442667321736818281375
     * 025450173378090483854166845232
     */
    [_I(19)] = {
        .expn = SEXP_2_D8EXP(-24), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/19! */
        .cells = { 8220635, 24662432, 97169559, 81236872, 28074922, 7389918, 26114134 }
    },
    /* 1/20! =
     * 0.0000000000000000004110317623312164858477990618436140374610369495913057067213336608684091406875
     * 1272508668904524192708342261600861987085352324885435075580
     */
    [_I(20)] = {
        .expn = SEXP_2_D8EXP(-24), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/20! */
        .cells = { 411031, 76233121, 64858477, 99061843, 61403746, 10369495, 91305707 }
    },
    /* 1/21! =
     * 0.000000000000000000019572941063391261230847574373505430355287473790062176510539698136590911461
     * 31012976603281167818700397250552421999385016777375496908360952830809216
     */
    [_I(21)] = {
        .expn = SEXP_2_D8EXP(-24), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/21! */
        .cells = { 19572, 94106339, 12612308, 47574373, 50543035, 52874737, 90062177 }
    },
    /* 1/22! =
     * 0.00000000000000000000088967913924505732867488974425024683433124880863918984138816809711776870278
     * 68240802742187126448638169320692827269931894
     */
    [_I(22)] = {
        .expn = SEXP_2_D8EXP(-24), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/22! */
        .cells = { 889, 67913924, 50573286, 74889744, 25024683, 43312488, 8639190 }
    },
    /* 1/23! =
     * 0.0000000000000000000000386817017063068403771691193152281232317934264625734713647029607442508131646
     * 445252293138570715158181274812731620431821497505038914695840480397078275699598837
     */
    [_I(23)] = {
        .expn = SEXP_2_D8EXP(-24), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/23! */
        .cells = { 38, 68170170, 63068403, 77169119, 31522812, 32317934, 26462573 }
    },
    /* 1/24! =
     * 0.0000000000000000000000016117375710961183490487133048011718013247261026072279735292900310104505485
     * 268552178880773779798257553117197150851
     */
    [_I(24)] = {
        .expn = SEXP_2_D8EXP(-24), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/24! */
        .cells = { 1, 61173757, 10961183, 49048713, 30480117, 18013247, 26102607 }
    },
    /* 1/25! =
     * 0.0000000000000000000000000644695028438447339619485321920468720529890441042891189411716012404180219
     * 410742087155230951191930302124687886034053035829175064857826400800661797126165998
     */
    [_I(25)] = {
        .expn = SEXP_2_D8EXP(-32), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/25! */
        .cells = { 6446950, 28438447, 33961948, 53219204, 68720529, 89044104, 28911894 }
    },
    /* 1/26! =
     * 0.00000000000000000000000000247959626322479746007494354584795661742265554247265842081429235540069315
     * 15797772582893498122766550081718764847463578301
     */
    [_I(26)] = {
        .expn = SEXP_2_D8EXP(-32), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/26! */
        .cells = { 247959, 62632247, 97460074, 94354584, 79566174, 22655542, 47265842 }
    },
    /* 1/27! =
     * 0.00000000000000000000000000009183689863795546148425716836473913397861687194343179336349230945928493
     * 153999175030701295601024648178414357350912436407823006621906358985764413064475299
     */
    [_I(27)] = {
        .expn = SEXP_2_D8EXP(-32), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/27! */
        .cells = { 9183, 68986379, 55461484, 25716836, 47391339, 78616871, 94343179 }
    },
    /* 1/28! =
     * 0.00000000000000000000000000000327988923706983791015204172731211192780774542655113547726758248068874
     * 7554999705368107605571794517206576556196754441574222502364966556780630
     */
    [_I(28)] = {
        .expn = SEXP_2_D8EXP(-32), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/28! */
        .cells = { 327, 98892370, 69837910, 15204172, 73121119, 27807745, 42655114 }
    },
    /* 1/29! =
     * 0.0000000000000000000000000000001130996288644771693155876457693831699244050147086598440437097407134
     * 0508810343811614164157144119024850263986885360143359387939189539850967690163872506526007013143054544564
     */
    [_I(29)] = {
        .expn = SEXP_2_D8EXP(-32), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/29! */
        .cells = { 11, 30996288, 64477169, 31558764, 57693831, 69924405, 147865 }
    },
    /* 1/30! =
     * 0.000000000000000000000000000000003769987628815905643852921525646105664146833823621994801456991357113
     * 502936781270538054719048039674950087995628453381119795979729846616989230
     */
    [_I(30)] = {
        .expn = SEXP_2_D8EXP(-40), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/31! */
        .cells = { 37699876, 28815905, 64385292, 15256461, 5664146, 83382362, 19948015 }
    },
    /* 1/31! =
     * 0.000000000000000000000000000000000121612504155351794962997468569229214972478510439419187143773914745
     * 59686892842808187273287251740886935767727833720584257406386225311667707193724594093
     */
    [_I(31)] = {
        .expn = SEXP_2_D8EXP(-40), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/31! */
        .cells = { 1216125, 4155351, 79496299, 74685692, 29214972, 47851043, 94191871 }
    },
    /* 1/32! =
     * 0.00000000000000000000000000000000000380039075485474359259367089278841296788995345123184959824293483
     * 57999021540133775585229022661690271674274149480376825804394956954
     */
    [_I(32)] = {
        .expn = SEXP_2_D8EXP(-40), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/32! */
        .cells = { 38003, 90754854, 74359259, 36708927, 88412967, 88995345, 12318496 }
    },
    /* 1/33! =
     * 0.000000000000000000000000000000000000115163356207719502805868814932982211148180407613086351461907
     * 11623636067133373871389463340200512203537658833175871765395271199076999685328781936168648710906456849803
     */
    [_I(33)] = {
        .expn = SEXP_2_D8EXP(-40), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/33! */
        .cells = { 1151, 63356207, 71950280, 58688149, 32982211, 14818040, 76130863 }
    },
    /* 1/34! =
     * 0.00000000000000000000000000000000000000338715753552116184723143573332300621024060022391430445476
     * 19740069517844509923151145480412354447657463702450517269898221385879638234368614064518143084443842520
     */
    [_I(34)] = {
        .expn = SEXP_2_D8EXP(-40), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/34! */
        .cells = { 33, 87157535, 52116184, 72314357, 33323006, 21024060, 2239143 }
    },
    /* 1/35! =
     * 0.000000000000000000000000000000000000000096775929586318909920898163809228748864017149254694412993
     * 19925734147955574263757470137260672699330703914985862077113777538822753781248175447
     */
    [_I(35)] = {
        .expn = SEXP_2_D8EXP(-48), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/35! */
        .cells = { 96775929, 58631890, 99208981, 63809228, 74886401, 71492546, 94412993 }
    },
    /* 1/36! =
     * 0.0000000000000000000000000000000000000000026882202662866363866916156613674652462226985904081781
     * 38699979370596654326184377075038127964638702973309718295021420493760784098272568937624168106594
     */
    [_I(36)] = {
        .expn = SEXP_2_D8EXP(-48), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/36! */
        .cells = { 2688220, 26628663, 63866916, 15661367, 46524622, 26985904, 8178139 }
    },
    /* 1/37! =
     * 0.0000000000000000000000000000000000000000000726546017915307131538274503072287904384513132542750
     * 848297291721782879547617399209469764314767217019813437377032816349665
     */
    [_I(37)] = {
        .expn = SEXP_2_D8EXP(-48), .sign = DEC_SIGN_PLUS, .ncells = DEC8_CELL_SIZE, /* 1/37! */
        .cells = { 72654, 60179153, 7131538, 27450307, 22879043, 84513132, 54275085 }
    }
};

static int32 cm_dec8_calc_prec(const dec8_t *dec);

static inline bool32 cm_dec8_taylor_break(const dec8_t *total, const dec8_t *delta, int32 prec)
{
    if (DECIMAL_IS_ZERO(delta)) {
        return GS_TRUE;
    }

    if (((int32)total->expn + (((total)->cells[0] >= 10000000) ? 1 : 0)) >
        (int32)SEXP_2_D8EXP(prec) + (int32)delta->expn) {
        return GS_TRUE;
    }
    return GS_FALSE;
}

static inline void cm_dec8_left_shift(const dec8_t *dec, uint32 offset, dec8_t *rs)
{
    uint32 ri, di;

    for (ri = 0, di = offset; di < (uint32)dec->ncells; ++ri, ++di) {
        rs->cells[ri] = dec->cells[di];
    }
    rs->ncells = (int8)((uint32)dec->ncells - offset);
    rs->expn = (int8)((uint32)dec->expn - offset);
}

/**
 * Right shift decimal cells. The leading cells are filled with zero.
 * @note The following conditions should be guaranteed by caller
 * + offset > 0 and offset < DEC_CELL_SIZE
 * + dec->ncells > 0
 */
static inline void cm_dec8_right_shift(const dec8_t *dec, int32 offset, dec8_t *rs)
{
    int32 di = dec->ncells - 1;
    int32 ri = di + offset;

    if (ri >= (DEC8_CELL_SIZE - 1)) {
        di -= (ri - (DEC8_CELL_SIZE - 1));
        ri = (DEC8_CELL_SIZE - 1);
    }

    rs->ncells = (uint8)(ri + 1);
    rs->sign = dec->sign;
    rs->expn = (int8)(dec->expn + offset);

    while (di >= 0) {
        rs->cells[ri] = dec->cells[di];
        ri--;
        di--;
    }

    while (ri >= 0) {
        rs->cells[ri] = 0;
        ri--;
    }
}

static inline void cm_dec8_rebuild(dec8_t *rs, uint32 cell0)
{
    /* decide the number of cells */
    if (rs->ncells < DEC8_CELL_SIZE) {
        rs->ncells++;
    }

    /* right shift cell data by 1 */
    uint32 i = rs->ncells;
    while (i-- > 1) {
        rs->cells[i] = rs->cells[i - 1];
    }

    /* put the carry into cells[0] */
    rs->cells[0] = (c8typ_t)cell0;
    rs->expn++;
}

/*
 * Truncate the tail of a decimal so that its precision is no more than prec
 * It must be that prec > 0
 */
status_t cm_dec8_finalise(dec8_t *dec, uint32 prec, bool32 allow_overflow)
{
    uint32 dpos; // position of truncating in decimal
    uint32 cpos; // the position of truncating in decimal->cells
    uint32 npos; // the position of truncating in decimal->cells[x]
    uint32 carry;
    int32 i;
    int32 sci_exp = DEC8_GET_SEXP(dec);

    // underflow check
    if (sci_exp < MIN_NUMERIC_EXPN) {
        cm_zero_dec8(dec);
        return GS_SUCCESS;
    }
    if (!allow_overflow) {
        DEC_OVERFLOW_CHECK_BY_SCIEXP(sci_exp);
    }

    GS_RETSUC_IFTRUE((uint32)dec->ncells <= (prec / DEC8_CELL_DIGIT));

    GS_RETVALUE_IFTRUE(((uint32)cm_dec8_calc_prec(dec) <= prec), GS_SUCCESS);

    dpos = (uint32)DEC8_POS_N_BY_PREC0(prec, cm_count_u32digits(dec->cells[0]));
    cpos = dpos / (uint32)DEC8_CELL_DIGIT;
    npos = dpos % (uint32)DEC8_CELL_DIGIT;
    carry = g_5ten_powers[DEC8_CELL_DIGIT - npos];

    dec->ncells = cpos + 1;
    for (i = (int32)cpos; i >= 0; --i) {
        dec->cells[i] += carry;
        carry = (dec->cells[i] >= DEC8_CELL_MASK);
        if (carry == 0) {
            break;
        }
        dec->cells[i] -= DEC8_CELL_MASK;
    }

    // truncate tailing digits to zeros
    dec->cells[cpos] /= g_1ten_powers[DEC8_CELL_DIGIT - npos];
    dec->cells[cpos] *= g_1ten_powers[DEC8_CELL_DIGIT - npos];

    if (carry > 0) {
        cm_dec8_rebuild(dec, 1);
        if (!allow_overflow) {
            DEC_OVERFLOW_CHECK_BY_SCIEXP(sci_exp + DEC8_CELL_DIGIT);
        }
    }

    (void)cm_dec8_trim_zeros(dec);
    return GS_SUCCESS;
}

/**
 * Product a cell array with the digit at pos (starting from left) is k
 */
static inline bool32 cm_dec8_make_round(const dec8_t *dec, uint32 pos, dec8_t *dx)
{
    int32 i;
    uint32 carry, j;

    cm_dec8_copy(dx, dec);
    if (pos >= DEC8_MAX_ALLOWED_PREC) {
        return GS_FALSE;
    }

    i = (int32)(pos / DEC8_CELL_DIGIT);
    j = pos % DEC8_CELL_DIGIT;

    carry = (uint32)g_5ten_powers[DEC8_CELL_DIGIT - j];
    for (; i >= 0; i--) {
        dx->cells[i] += carry;
        carry = (dx->cells[i] >= DEC8_CELL_MASK);
        if (!carry) {
            return GS_FALSE;
        }
        dx->cells[i] -= DEC8_CELL_MASK;
    }

    if (carry > 0) {
        cm_dec8_rebuild(dx, 1);
    }

    return carry;
}

// whether abs(dec) is equal to 1
static inline bool32 cm_dec8_is_absolute_one(const dec8_t *dec)
{
    return (bool32)(dec->ncells == 1 && dec->cells[0] == 1 && dec->expn == 0);
}

//  whether dec is equal to 1
static inline bool32 cm_dec8_is_one(const dec8_t *d8)
{
    return (bool32)(d8->ncells == 1 && d8->cells[0] == 1 && d8->sign == DEC_SIGN_PLUS && d8->expn == 0);
}

// check whether abs(x) is greater than 1
static inline bool32 dec8_is_greater_than_one(const dec8_t *x)
{
    if (x->expn > 0) {
        return GS_TRUE;
    }

    if (x->expn == 0) {
        if (x->ncells > 1) {
            return GS_TRUE;
        }
        if (x->ncells == 1 && x->cells[0] > 1) {
            return GS_TRUE;
        }
    }

    return GS_FALSE;
}

static inline void cm_add_aligned_dec8(const dec8_t *d1, const dec8_t *d2, dec8_t *rs)
{
    uint32 i;
    c8typ_t carry = 0;

    if (d1->ncells > d2->ncells) {
        SWAP(const dec8_t *, d1, d2);
    }

    i = d2->ncells;
    while (i > (uint32)d1->ncells) {
        i--;
        rs->cells[i] = d2->cells[i];
    }
    rs->head = d2->head;

    while (i-- > 0) {
        rs->cells[i] = d1->cells[i] + d2->cells[i] + carry;
        carry = (rs->cells[i] >= DEC8_CELL_MASK); // carry can be either 1 or 0 in addition
        if (carry) {
            rs->cells[i] -= DEC8_CELL_MASK;
        }
    }

    if (carry) {
        cm_dec8_rebuild(rs, 1);
    }

    (void)cm_dec8_trim_zeros(rs);
}

/* * Subtraction of two cell array. large must greater than small.  */
static inline void cm_sub_aligned_dec8(const dec8_t *large, const dec8_t *small, bool32 flip_sign, dec8_t *rs)
{
    /* if small has more cells than large, a borrow must be happened */
    int32 borrow = (small->ncells > large->ncells) ? 1 : 0;
    uint32 i;

    if ((bool32)borrow) {
        i = small->ncells - 1;
        rs->cells[i] = DEC8_CELL_MASK - small->cells[i];
        while (i > (uint32)large->ncells) {
            i--;
            rs->cells[i] = (DEC8_CELL_MASK - 1) - small->cells[i];
        }
        rs->ncells = small->ncells;
    } else {
        i = large->ncells;
        while (i > (uint32)small->ncells) {
            i--;
            rs->cells[i] = large->cells[i];
        }
        rs->ncells = large->ncells;
    }

    while (i-- > 0) {
        int32 tmp = (int32)(large->cells[i] - (small->cells[i] + borrow));
        borrow = (tmp < 0); // borrow can be either 1 or 0
        if (borrow) {
            tmp += (int32)DEC8_CELL_MASK;
        }
        rs->cells[i] = (c8typ_t)tmp;
    }

    rs->expn = large->expn;
    rs->sign = flip_sign ? NEGATE_SIGN(large->sign) : large->sign;

    if (rs->cells[0] == 0) {
        for (i = 1; i < (uint32)rs->ncells; i++) {
            if (rs->cells[i] > 0) {
                break;
            }
        }
        cm_dec8_left_shift(rs, i, rs);
    }

    (void)cm_dec8_trim_zeros(rs);
}

/**
* Quickly find the precision of a cells
* @note  (1) The cell u0 should be specially treated;
*        (2) The tailing zeros will not be counted. If all cell except u0 are
*        zeros, then the precision of u0 is re-counted by ignoring tailing zeros
*        e.g. | u0 = 1000 | u1 = 0 | u2 = 0 |..., the precision 1 will be
*        returned.

*/
static int32 cm_dec8_calc_prec(const dec8_t *dec)
{
    int32 i, j;
    uint32 u;
    int32 prec = 0;

    if (dec->ncells == 0) {
        return 0;
    }

    /* Step 1: Find the precision of remaining cells starting from backend */
    for (i = dec->ncells - 1; i > 0; --i) {
        if (dec->cells[i] > 0) { // found the last non-zero cell (dec->cells[i]>0)
            // count digits in this cell by ignoring tailing zeros
            j = 0;
            u = dec->cells[i];
            while (u % 10 == 0) {
                ++j;
                u /= 10;
            }
            prec += (i * DEC8_CELL_DIGIT - j);
            break;
        }
    }

    /* Step 1: Count the precision of u0 */
    if (i == 0) { // if u1, u2, ... are zeros, then the precision of u0 should remove tailing zeros
        u = dec->cells[0];
        while (u % 10 == 0) { // remove tailing zeros
            u /= 10;
        }
        prec = (int32)cm_count_u32digits((c8typ_t)u);
    } else {
        prec += (int32)cm_count_u32digits(dec->cells[0]);
    }

    return prec;
}

/**
 * Convert the significant digits of cells into text with a maximal len
 * @note  The tailing zeros are removed when outputting
 */
static void cm_cell8s_to_text(const cell8_t cells, uint32 ncell, text_t *text, int32 max_len)
{
    uint32 i;
    int iret_snprintf;

    iret_snprintf = snprintf_s(text->str, DEC8_CELL_DIGIT + 1, DEC8_CELL_DIGIT, "%u", cells[0]);
    PRTS_RETVOID_IFERR(iret_snprintf);
    text->len = (uint32)iret_snprintf;
    for (i = 1; (text->len < (uint32)max_len) && (i < ncell); ++i) {
        iret_snprintf =
            snprintf_s(CM_GET_TAIL(text), DEC8_CELL_DIGIT + 1, DEC8_CELL_DIGIT, DEC8_CELL_FMT, (uint32)cells[i]);
        PRTS_RETVOID_IFERR(iret_snprintf);
        text->len += (uint32)iret_snprintf;
    }

    // truncate redundant digits
    if (text->len > (uint32)max_len) {
        text->len = (uint32)max_len;
    }

    // truncate tailing zeros
    for (i = (uint32)text->len - 1; i > 0; --i) {
        if (!CM_IS_ZERO(text->str[i])) {
            break;
        }
        --text->len;
    }

    CM_NULL_TERM(text);
}

/**
 * Round a decimal to a text with the maximal length max_len
 * If the precision is greater than max_len, a rounding mode is used.
 * The rounding mode may cause a change on precision, e.g., the 8-precision
 * decimal 99999.999 rounds to 7-precision decimal is 100000.00, and then
 * its actual precision is 8. The function will return the change. If
 * no change occurs, zero is returned.
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller, i.g. 1.max_len > 0    2.dec->cells[0] > 0
 */
static int32 cm_dec8_round_to_text(const dec8_t *dec, int32 max_len, text_t *text_out)
{
    dec8_t txtdec;
    uint32 prec_u0;
    int32 prec;

    prec = cm_dec8_calc_prec(dec);
    if (prec <= max_len) { // total prec under the max_len
        cm_cell8s_to_text(dec->cells, dec->ncells, text_out, prec);
        return 0;
    }

    /* * if prec > max_len, the rounding mode is applied */
    prec_u0 = cm_count_u32digits(dec->cells[0]);
    // Rounding model begins by adding with {5[(prec - max_len) zeros]}
    // Obtain the pos of 5 for rounding, then prec is used to represent position
    prec = DEC8_POS_N_BY_PREC0(max_len, prec_u0);
    // add for rounding and check whether the carry happens, and capture the changes of the precision
    if (cm_dec8_make_round(dec, (uint32)prec, &txtdec)) {
        // if carry happens, the change must exist
        cm_cell8s_to_text(txtdec.cells, dec->ncells + 1, text_out, max_len);
        return 1;
    } else {
        cm_cell8s_to_text(txtdec.cells, dec->ncells, text_out, max_len);
        return (cm_count_u32digits(txtdec.cells[0]) > prec_u0) ? 1 : 0;
    }
}


/*
 * Convert a cell text into a cell of big integer by specifying the
 * length digits in u0 (i.e., len_u0), and return the number of non-zero cells
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller, i.g. cells[0] > 0
 */
static inline int32 cm_digitext_to_cell8s(digitext_t *dtext, cell8_t cells, int32 len_u0)
{
    uint32 i, k;
    text_t cell_text;

    // make u0
    cell_text.str = dtext->str;
    cell_text.len = (uint32)len_u0;
    cells[0] = (c8typ_t)cm_celltext2uint32(&cell_text);

    // make u1, u2, ..., uk
    k = 1;
    for (i = (uint32)len_u0; k < DEC8_CELL_SIZE && i < dtext->len; k++) {
        cell_text.str = dtext->str + i;
        cell_text.len = (uint32)DEC8_CELL_DIGIT;
        cells[k] = (c8typ_t)cm_celltext2uint32(&cell_text);
        i += DEC8_CELL_DIGIT;
    }

    // the tailing cells of significant cells may be zeros, for returning
    // accurate ncells, they should be ignored.
    while (cells[k - 1] == 0) {
        --k;
    }

    return (int32)k;
}

/**
 * Convert a digit text with a scientific exponent into a decimal
 * The digit text may be changed when adjust the scale of decimal to be
 * an integral multiple of DEC_CELL_DIGIT, by appending zeros.
 * @return the precision of u0
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller,
 * i.g. dtext->len > 0 && dtext->len <= (uint32)DEC_MAX_ALLOWED_PREC
 */
static inline int32 cm_digitext_to_dec8(dec8_t *dec, digitext_t *dtext, int32 sci_exp)
{
    int32 delta;
    int32 len_u0; // the length of u0

    len_u0 = (int32)dtext->len % DEC8_CELL_DIGIT;

    ++sci_exp; // increase the sci_exp to obtain the position of dot

    delta = sci_exp - len_u0;
    delta += (int32)DEC8_CELL_DIGIT << 16; // make delta to be positive
    delta %= DEC8_CELL_DIGIT;              // get the number of appending zeros
    len_u0 = (len_u0 + delta) % DEC8_CELL_DIGIT;

    if (len_u0 == 0) {
        len_u0 = DEC8_CELL_DIGIT;
    }

    while (delta-- > 0) {
        CM_TEXT_APPEND(dtext, '0');
    }

    CM_NULL_TERM(dtext);

    dec->ncells = (uint8)cm_digitext_to_cell8s(dtext, dec->cells, len_u0);
    dec->expn = SEXP_2_D8EXP(sci_exp - len_u0);
    return len_u0;
}

#define DEC_EXPN_BUFF_SZ 16
/**
 * Output a decimal type in scientific format, e.g., 2.34566E-20
 */
static inline status_t cm_dec8_to_sci_text(text_t *text, const dec8_t *dec, int32 max_len)
{
    int32 i;
    char obuff[GS_NUMBER_BUFFER_SIZE]; /* * output buff */
    text_t cell_text = {
        .str = obuff,
        .len = 0
    };
    char sci_buff[DEC_EXPN_BUFF_SZ];
    int32 sci_exp; /* * The scientific scale of the dec */
    int32 placer;
    int iret_snprintf;

    sci_exp = DEC8_GET_SEXP(dec);
    // digits of sci_exp + sign(dec) + dot + E + sign(expn)
    placer = (int32)dec->sign + 3;
    placer += (int32)cm_count_u32digits((c8typ_t)abs(sci_exp));

    if (max_len <= placer) {
        return GS_ERROR;
    }

    /* The round of a decimal may increase the precision by 1 */
    if (cm_dec8_round_to_text(dec, max_len - placer, &cell_text) > 0) {
        ++sci_exp;
    }
    // compute the exponent placer
    iret_snprintf = snprintf_s(sci_buff, DEC_EXPN_BUFF_SZ, DEC_EXPN_BUFF_SZ - 1, "E%+d", sci_exp);
    PRTS_RETURN_IFERR(iret_snprintf);
    placer = iret_snprintf;

    // Step 1. output sign
    text->len = 0;
    if (dec->sign == DEC_SIGN_MINUS) {
        CM_TEXT_APPEND(text, '-');
    }

    CM_TEXT_APPEND(text, cell_text.str[0]);
    CM_TEXT_APPEND(text, '.');
    for (i = 1; (int32)text->len < max_len - placer; ++i) {
        if (i < (int32)cell_text.len) {
            CM_TEXT_APPEND(text, cell_text.str[i]);
        } else {
            CM_TEXT_APPEND(text, '0');
        }
    }
    iret_snprintf = snprintf_s(&text->str[text->len], DEC_EXPN_BUFF_SZ, DEC_EXPN_BUFF_SZ - 1, "%s", sci_buff);
    PRTS_RETURN_IFERR(iret_snprintf);
    text->len += (uint32)iret_snprintf;
    return GS_SUCCESS;
}

/**
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller, i.g. dot_pos <= max_len - dec->sign
 */
static inline status_t cm_dec8_to_plain_text(text_t *text, const dec8_t *dec, int32 max_len, int32 sci_exp, int32 prec)
{
    int32 dot_pos;
    char obuff[GS_NUMBER_BUFFER_SIZE]; /* * output buff */
    text_t cell_text;
    cell_text.str = obuff;
    cell_text.len = 0;

    // clear text & output sign
    text->len = 0;
    if (dec->sign == DEC_SIGN_MINUS) {
        CM_TEXT_APPEND(text, '-');
    }

    dot_pos = sci_exp + 1;

    if (prec <= dot_pos) {
        (void)cm_dec8_round_to_text(dec, max_len - dec->sign, &cell_text); // subtract sign
        cm_concat_text(text, max_len, &cell_text);
        cm_text_appendc(text, dot_pos - prec, '0');
        CM_NULL_TERM(text);
        return GS_SUCCESS;
    }

    /* get the position of dot w.r.t. the first significant digit */
    if (dot_pos == max_len - dec->sign) {
        /* handle the border case with dot at the max_len position,
         * then the dot is not outputted. Suppose max_len = 10,
         * (1). 1234567890.222 --> 1234567890 is outputted
         * If round mode products carry, e.g. the rounded value of
         * 9999999999.9 is 10000000000, whose length is 11 and greater than
         * max_len, then the scientific format is used to print the decimal
         */
        if (cm_dec8_round_to_text(dec, dot_pos, &cell_text) > 0) {
            CM_TEXT_CLEAR(text);
            return cm_dec8_to_sci_text(text, dec, max_len);
        }
        cm_concat_text(text, max_len, &cell_text);
        cm_text_appendc(text, max_len - (int32)text->len, '0');
    } else if (dot_pos == max_len - dec->sign - 1) {
        /* handle the border case with dot at the max_len - 1 position,
         * then only max_len-1 is print but the dot is emitted. Assume
         * max_len = 10, the following cases output:
         * (1). 123456789.2345 ==> 123456789  (.2345 is abandoned)
         * (2). 987654321.56   ==> 987654322  (.56 is rounded to 1)
         * If a carry happens, e.g., 999999999.6 ==> 1000000000, max_len
         * number of digits will be printed.
         *  */
        int32 change = cm_dec8_round_to_text(dec, dot_pos, &cell_text);
        cm_concat_text(text, max_len, &cell_text);
        cm_text_appendc(text, max_len + change - ((int32)text->len + 1), '0');
    } else if (dot_pos >= 0) { /* dot is inside of cell_text and may be output */
        // round mode may product carry, and thus may affect the dot_pos
        dot_pos += cm_dec8_round_to_text(dec, max_len - dec->sign - 1, &cell_text); // subtract sign & dot
        if ((int32)cell_text.len <= dot_pos) {
            cm_concat_text(text, max_len, &cell_text);
            cm_text_appendc(text, dot_pos - (int32)cell_text.len, '0');
        } else {
            GS_RETURN_IFERR(cm_concat_ntext(text, &cell_text, dot_pos));
            CM_TEXT_APPEND(text, '.');
            // copy remaining digits
            cell_text.str += (uint32)dot_pos;
            cell_text.len -= (uint32)dot_pos;
            cm_concat_text(text, max_len, &cell_text);
        }
    } else { // dot_pos is less than 0
        /* dot is in the most left & add |dot_pos| zeros between dot and cell_text
         * Thus, the maxi_len should consider sign, dot, and the adding zeros */
        dot_pos += cm_dec8_round_to_text(dec, max_len - dec->sign - 1 + dot_pos, &cell_text);
        CM_TEXT_APPEND(text, '.');
        cm_text_appendc(text, -dot_pos, '0');
        GS_RETURN_IFERR(cm_concat_ntext(text, &cell_text, max_len - (int32)text->len));
    }

    CM_NULL_TERM(text);
    return GS_SUCCESS;
}

/**
 * Convert a decimal into a text with a given maximal precision
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller,
 * i.g. 1.dec->sign == DEC_SIGN_PLUS    2.dec->expn == 0    3.dec->cells[0] > 0
 */
status_t cm_dec8_to_text(const dec8_t *dec, int32 max_len, text_t *text)
{
    int32 sci_exp; /* * The scientific scale of the dec */
    int32 prec;

    CM_POINTER2(dec, text);
    max_len = MIN(max_len, (int32)(GS_NUMBER_BUFFER_SIZE - 1));

    if (dec->ncells == 0) {
        text->str[0] = '0';
        text->len = 1;
        return GS_SUCCESS;
    }

    // Compute the final scientific scale of the dec, i.e., format of d.xxxx , d > 0.
    // Each decimal has an unique scientific representation.
    sci_exp = DEC8_GET_SEXP(dec);
    // get the total precision of the decimal
    prec = cm_dec8_calc_prec(dec);
    // Scientific representation when the scale exceeds the maximal precision
    // or have many leading zeros and have many significant digits
    // When sci_exp < 0, the length for '.' should be considered
    if ((sci_exp < -6 && -sci_exp + prec + (int32)dec->sign > max_len) ||
        (sci_exp > 0 && sci_exp + 1 + (int32)dec->sign > max_len)) {
        return cm_dec8_to_sci_text(text, dec, max_len);
    }

    // output plain text
    return cm_dec8_to_plain_text(text, dec, max_len, sci_exp, prec);
}

status_t cm_str_to_dec8(const char *str, dec8_t *dec)
{
    text_t text;
    cm_str2text((char *)str, &text);
    return cm_text_to_dec8(&text, dec);
}

static inline void cm_do_numpart_round8(const num_part_t *np, dec8_t *dec, uint32 prec0)
{
    c8typ_t carry = g_1ten_powers[prec0 % DEC8_CELL_DIGIT];
    uint32 i = dec->ncells;

    while (i-- > 0) {
        dec->cells[i] += carry;
        carry = (dec->cells[i] >= DEC8_CELL_MASK);
        if (carry == 0) {
            return;
        }
        dec->cells[i] -= DEC8_CELL_MASK;
    }

    if (carry > 0) {
        cm_dec8_rebuild(dec, 1);
    }
}

num_errno_t cm_numpart_to_dec8(num_part_t *np, dec8_t *dec)
{
    if (NUMPART_IS_ZERO(np)) {
        cm_zero_dec8(dec);
        return NERR_SUCCESS;
    }

    // Step 3.2. check overflow by comparing scientific scale and MAX_NUMERIC_EXPN
    if (np->sci_expn > MAX_NUMERIC_EXPN) { // overflow return Error
        return NERR_OVERFLOW;
    } else if (np->sci_expn < MIN_NUMERIC_EXPN) { // underflow return 0
        cm_zero_dec8(dec);
        return NERR_SUCCESS;
    }

    // Step 4: make the final decimal value
    dec->sign = (uint8)np->is_neg;
    int32 prec0 = cm_digitext_to_dec8(dec, &np->digit_text, np->sci_expn);

    if (np->do_round) { // when round happens, the dec->cells should increase 1
        cm_do_numpart_round8(np, dec, (uint32)prec0);
        cm_dec8_trim_zeros(dec); // rounding may change the precision
    }

    return NERR_SUCCESS;
}

/**
 * Translates a text_t representation of a decimal into a decimal
 * @param
 * -- precision: records the precision of the decimal text. The initial value
 * is -1, indicating no significant digit found. When a leading zero
 * is found, the precision is set to 0, it means the merely
 * significant digit is zero. precision > 0 represents the
 * number of significant digits in the decimal text.
 */
status_t cm_text_to_dec8(const text_t *dec_text, dec8_t *dec)
{
    num_errno_t err_no;
    num_part_t np;
    np.excl_flag = NF_NONE;

    err_no = cm_split_num_text(dec_text, &np);
    if (err_no != NERR_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_NUMBER, cm_get_num_errinfo(err_no));
        return GS_ERROR;
    }

    err_no = cm_numpart_to_dec8(&np, dec);
    if (err_no != NERR_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_NUMBER, cm_get_num_errinfo(err_no));
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t cm_hext_to_dec8(const text_t *hex_text, dec8_t *dec)
{
    uint32 i;
    uint8 u8;

    static const dec8_t DEC8_16 = {
        .expn = SEXP_2_D8EXP(0),
        .sign = DEC_SIGN_PLUS,
        .ncells = (uint8)1,
        .cells = { 16 }
    };

    if (CM_IS_EMPTY(hex_text)) {
        GS_THROW_ERROR(ERR_INVALID_NUMBER, cm_get_num_errinfo(NERR_NO_DIGIT));
        return GS_ERROR;
    }

    cm_zero_dec8(dec);

    for (i = 0; i < hex_text->len; i++) {
        u8 = cm_hex2int8((uchar)hex_text->str[i]);
        if (u8 == (uint8)0xFF) {
            GS_THROW_ERROR(ERR_INVALID_NUMBER, cm_get_num_errinfo(NERR_UNEXPECTED_CHAR));
            return GS_ERROR;
        }
        GS_RETURN_IFERR(cm_dec8_multiply(dec, &DEC8_16, dec));
        GS_RETURN_IFERR(cm_dec8_add_int32(dec, u8, dec));
    }

    return GS_SUCCESS;
}

/**
 * Fill a non-zero uint32 into decimal
 * @note u64 > 0
 */
static inline void cm_fill_uint32_into_dec8(uint32 u32, dec8_t *dec)
{
    if (u32 < DEC8_CELL_MASK) {
        dec->expn = SEXP_2_D8EXP(0);
        dec->ncells = 1;
        dec->cells[0] = (c8typ_t)u32;
        return;
    } else {
        // u32 is greater than or equal to 10^8
        // u32 is less than 10^8
        dec->expn = SEXP_2_D8EXP(DEC8_CELL_DIGIT);
        dec->cells[0] = (c8typ_t)(u32 / DEC8_CELL_MASK);
        dec->cells[1] = (c8typ_t)(u32 % DEC8_CELL_MASK);
        dec->ncells = (dec->cells[1] > 0) ? 2 : 1;
    }
}

/**
 * Convert an integer32 into a decimal
 */
void cm_int32_to_dec8(int32 i32, dec8_t *dec)
{
    if (i32 > 0) {
        dec->sign = DEC_SIGN_PLUS;
    } else if (i32 < 0) {
        if (i32 == GS_MIN_INT32) {
            cm_dec8_copy(dec, &DEC8_MIN_INT32);
            return;
        }
        dec->sign = DEC_SIGN_MINUS;
        i32 = -i32;
    } else {
        cm_zero_dec8(dec);
        return;
    }

    cm_fill_uint32_into_dec8((uint32)i32, dec);
}

void cm_uint32_to_dec8(uint32 i32, dec8_t *dec)
{
    if (i32 == 0) {
        cm_zero_dec8(dec);
        return;
    }

    dec->sign = DEC_SIGN_PLUS;
    cm_fill_uint32_into_dec8(i32, dec);
}


/* * The buffer size to covert an int64 to dec->cells. It must be greater
* * max_digits(int64) + DEC_CELL_DIGIT + 1  than */
#define INT64_BUFF 32

/*
 * Fill a non-zero uint64(u64 > 0) into decimal
 */
static inline void cm_fill_uint64_into_dec8(uint64 u64, dec8_t *dec)
{
    if (u64 < DEC8_POW2_MASK) {
        if (u64 < DEC8_CELL_MASK) {
            dec->expn = SEXP_2_D8EXP(0);
            dec->ncells = 1;
            dec->cells[0] = (c8typ_t)u64;
        } else {
            dec->expn = SEXP_2_D8EXP(DEC8_CELL_DIGIT);
            dec->cells[0] = (c8typ_t)(u64 / DEC8_CELL_MASK);
            dec->cells[1] = (c8typ_t)(u64 % DEC8_CELL_MASK);
            dec->ncells = dec->cells[1] > 0 ? 2 : 1;
        }
        return;
    }

    dec->expn = SEXP_2_D8EXP(DEC8_CELL_DIGIT * 2);
    dec->cells[0] = (c8typ_t)(u64 / DEC8_POW2_MASK);
    u64 %= DEC8_POW2_MASK;
    dec->cells[1] = (c8typ_t)(u64 / DEC8_CELL_MASK);
    dec->cells[2] = (c8typ_t)(u64 % DEC8_CELL_MASK);
    dec->ncells = dec->cells[2] > 0 ? 3 : (dec->cells[1] > 0 ? 2 : 1);
}

/**
 * Convert an integer64 into a decimal
 */
void cm_int64_to_dec8(int64 i64, dec8_t *dec)
{
    if (i64 > 0) {
        dec->sign = DEC_SIGN_PLUS;
    } else if (i64 < 0) {
        if (i64 == GS_MIN_INT64) {
            cm_dec8_copy(dec, &DEC8_MIN_INT64);
            return;
        }
        dec->sign = DEC_SIGN_MINUS;
        i64 = -i64;
    } else {
        cm_zero_dec8(dec);
        return;
    }

    cm_fill_uint64_into_dec8((uint64)i64, dec);
}

#define cm_int64_to_dec(i64, dec) cm_int64_to_dec8((i64), (dec))

/**
 * Convert an uint64 into a decimal
 * @author 2018/06/05
 */
void cm_uint64_to_dec8(uint64 u64, dec8_t *dec)
{
    if (u64 == 0) {
        cm_zero_dec8(dec);
        return;
    }
    dec->sign = DEC_SIGN_PLUS;
    cm_fill_uint64_into_dec8(u64, dec);
}

static const double g_pos_pow8[] = {
    1.0,
    1.0e8,
    1.0e16,
    1.0e24,
    1.0e32,
    1.0e40,
    1.0e48,
    1.0e56,
    1.0e64,
    1.0e72,
    1.0e80,
    1.0e88,
    1.0e96,
    1.0e104,
    1.0e112,
    1.0e120,
    1.0e128,
    1.0e136,
    1.0e144,
    1.0e152,
    1.0e160,
};

/**
 * compute 100000000^x, x should be between -21 and 21
 */
static inline double cm_pow8(int32 x)
{
    int32 y = abs(x);
    double r = (y < 21) ? g_pos_pow8[y] : pow(10e8, y);
    if (x < 0) {
        r = 1.0 / r;
    }
    return r;
}

static status_t cm_real_to_dec8_inexac(double r, dec8_t *dec);

/**
 * Convert real value into a decimal type
 */
status_t cm_real_to_dec8(double real, dec8_t *dec)
{
    GS_RETURN_IFERR(cm_real_to_dec8_inexac(real, dec));
    // reserving at most GS_MAX_REAL_PREC precisions
    return cm_dec8_finalise(dec, GS_MAX_REAL_PREC, GS_FALSE);
}

/**
 * Convert real value into a decimal type 10 precisions
 */
status_t cm_real_to_dec8_prec10(double real, dec8_t *dec)
{
    GS_RETURN_IFERR(cm_real_to_dec8_inexac(real, dec));
    return cm_dec8_finalise(dec, GS_MAX_INDEX_REAL_PREC, GS_FALSE);
}

/**
 * Convert real value into a decimal. It is similar with the function cm_real_to_dec8.
 * This function may be more efficient than cm_real_to_dec8, but may lose precision.
 * It is suitable for an algorithm which needs an inexact initial value.
 */
static status_t cm_real_to_dec8_inexac(double r, dec8_t *dec)
{
    if (!CM_DBL_IS_FINITE(r)) {
        GS_THROW_ERROR(ERR_INVALID_NUMBER, "");
        return GS_ERROR;
    }

    if (r == 0.0) {
        cm_zero_dec8(dec);
        return GS_SUCCESS;
    }

    double int_r;
    int32 dexp;

    bool32 is_neg = (r < 0);
    if (is_neg) {
        r = -r;
    }

    // compute an approximate scientific exponent
    (void)frexp(r, &dexp);
    dexp = (int32)((double)dexp * (double)GS_LOG10_2);
    dexp &= 0xFFFFFFF8;

    // Set a decimal
    dec->expn = SEXP_2_D8EXP(dexp);
    dec->sign = is_neg ? DEC_SIGN_MINUS : DEC_SIGN_PLUS;

    r *= cm_pow8(-dec->expn);
    // now, int_r is used as the integer part of r
    if (r >= 1.0) {
        r = modf(r, &int_r);
        dec->cells[0] = (c8typ_t)((int32)int_r);
        dec->ncells = 1;
    } else {
        dec->ncells = 0;
        --dec->expn;
    }

    while (dec->ncells < 3) {
        if (r == 0) {
            break;
        }
        r = modf(r * (double)DEC8_CELL_MASK, &int_r);
        dec->cells[dec->ncells++] = (c8typ_t)((int32)int_r);
    }
    cm_dec8_trim_zeros(dec);
    return GS_SUCCESS;
}

/**
 * NOTE THAT: convert a signed integer into DOUBLE is faster than unsigned integer,
 * therefore, These codes use signed integer for conversation to DOUBLE as much as
 * possible. The following SWITCH..CASE is faster than the loop implementation.
 */
double cm_dec8_to_real(const dec8_t *dec)
{
    if (DECIMAL_IS_ZERO(dec)) {
        return 0.0;
    }

    double dval;
    int32 i = MIN(dec->ncells, 3);
    uint64 u64;

    if (i == 3) {
        u64 = (uint64)dec->cells[1] * DEC8_CELL_MASK + (uint64)dec->cells[2];
        dval = (double)(int64)dec->cells[0] * (double)DEC8_POW2_MASK;
        dval += (double)(int64)u64;
    } else if (i == 2) {
        u64 = (uint64)dec->cells[0] * DEC8_CELL_MASK + (uint64)dec->cells[1];
        dval = (double)((int64)u64);
    } else {
        dval = (int32)dec->cells[0];
    }

    int32 dexpn = (int32)dec->expn - i + 1;

    /* the maximal expn of a decimal can not exceed 21 */
    if (dexpn >= 0) {
        dval *= g_pos_pow8[dexpn];
    } else {
        dval /= g_pos_pow8[-dexpn];
    }

    return DEC_IS_NEGATIVE(dec) ? -dval : dval;
}


/**
 * The core algorithm for adding of two decimals, without truncating
 * the result.
 * @see cm_decimal_add for adding of two decimals with truncation
 */
status_t cm_dec8_add_op(const dec8_t *d1, const dec8_t *d2, dec8_t *rs)
{
    int32 offset;
    dec8_t calc_dec;
    bool32 is_same_sign;

    // Ensure the scales of two adding decimal to be even multiple of DEC_CELL_DIGIT
    if (DECIMAL_IS_ZERO(d2)) {
        goto DEC8_ADD_ZERO;
    }

    if (DECIMAL_IS_ZERO(d1)) {
        d1 = d2;
        goto DEC8_ADD_ZERO;
    }

    // Obtain the exponent offset of two decimals
    offset = (int32)d1->expn - (int32)d2->expn; // exponent offset
    is_same_sign = (d1->sign == d2->sign);

    if (offset != 0) {
        if (offset < 0) {
            /* offset < 0 means d1 < d2, then swap d1 and d2 to grant d1 > d2 */
            offset = -offset;
            SWAP(const dec8_t *, d1, d2);
        }

        if (offset >= DEC8_MAX_EXP_OFFSET) {
            goto DEC8_ADD_ZERO;
        }

        cm_dec8_right_shift(d2, offset, &calc_dec);
        d2 = &calc_dec;
    } else if (!is_same_sign) { // if offset == 0, and d1, d2 have different signs
        int32 cmp = cm_dec8_cmp_data(d1, d2, 1);
        if (cmp < 0) {
            SWAP(const dec8_t *, d1, d2);
        } else if (cmp == 0) {
            cm_zero_dec8(rs);
            return GS_SUCCESS;
        }
    }

    if (is_same_sign) {
        cm_add_aligned_dec8(d1, d2, rs);
    } else {
        cm_sub_aligned_dec8(d1, d2, GS_FALSE, rs);
    }
    return GS_SUCCESS;

DEC8_ADD_ZERO:
    cm_dec8_copy(rs, d1);
    return GS_SUCCESS;
}


/**
 * The core algorithm for subtracting of two decimals, without truncating
 * the result.
 * @see cm_decimal_sub for subtraction of two decimals with truncation
 */
status_t cm_dec8_sub_op(const dec8_t *d1, const dec8_t *d2, dec8_t *rs)
{
    dec8_t calc_dec;
    int32 offset;
    bool32 do_swap = GS_FALSE;
    bool32 is_same_sign;

    if (DECIMAL_IS_ZERO(d2)) {
        goto DEC8_SUB_ZERO;
    }

    if (DECIMAL_IS_ZERO(d1)) {
        do_swap = GS_TRUE;
        d1 = d2;
        goto DEC8_SUB_ZERO;
    }

    // Obtain the exponent offset of two decimals
    offset = (int32)d1->expn - (int32)d2->expn; // exponent offset
    is_same_sign = (d1->sign == d2->sign);

    if (offset != 0) {
        if (offset < 0) {
            offset = -offset;
            SWAP(const dec8_t *, d1, d2);
            do_swap = GS_TRUE;
        }

        if (offset >= DEC8_MAX_EXP_OFFSET) {
            goto DEC8_SUB_ZERO;
        }

        cm_dec8_right_shift(d2, offset, &calc_dec);
        d2 = &calc_dec;
    } else if (is_same_sign) {
        int32 cmp = cm_dec8_cmp_data(d1, d2, 1);
        if (cmp < 0) {
            SWAP(const dec8_t *, d1, d2);
            do_swap = GS_TRUE;
        } else if (cmp == 0) {
            cm_zero_dec8(rs);
            return GS_SUCCESS;
        }
    }

    if (is_same_sign) {
        cm_sub_aligned_dec8(d1, d2, do_swap, rs);
    } else {
        /* if d1 and d2 have different signs, the result sign is the same with
         * the first operand. */
        uint8 sign = do_swap ? d2->sign : d1->sign;
        cm_add_aligned_dec8(d1, d2, rs);
        rs->sign = sign;
    }
    return GS_SUCCESS;

DEC8_SUB_ZERO:
    cm_dec8_copy(rs, d1);
    if (do_swap && !DECIMAL_IS_ZERO(rs)) {
        rs->sign = NEGATE_SIGN(rs->sign);
    }
    return GS_SUCCESS;
}

/**
 * The core algorithm for multiplying of two decimals, without truncating
 * the result.
 * @see cm_dec8_multiply for multiplying of two decimals with truncation
 */
status_t cm_dec8_mul_op(const dec8_t *d1, const dec8_t *d2, dec8_t *rs)
{
    if (DECIMAL_IS_ZERO(d1) || DECIMAL_IS_ZERO(d2)) {
        cm_zero_dec8(rs);
        return GS_SUCCESS;
    }

    uint32 i, j, n;
    cc8typ_t carry = 0;

    /* Step 1: product an initial carry, the following facts is true:
     * (1) 1<= j < n1, n1 <= N
     * (2) 0 <= i < n2, n2 <= N
     * (3) i = N - j,
     * in which j and i is the index to access d1 and d2 respectively; N is DEC_CELL_SIZE.
     * By these facts, it can derive that: (N - n2 + 1) <= j < n1.
     * In following code block, j starts from (N - n2 + 1) towards to n1, the maximal step
     * is delta = n1 - (N - n2 + 1); while i begins with (n2 - 1) and is decreasing
     * to -INFINITY. Thus, the minimal value of i is
     * n2 - 1 - delta = n2 - 1 - {n1 - (N - n2 + 1)} = N - n1 >= 0,
     * Therefore, cross-border accessing to d1 can not be happened!!
     *  */
    i = d2->ncells - 1;
    j = DEC8_CELL_SIZE - i;
    for (; j < (uint32)d1->ncells; j++, i--) {
        carry += (cc8typ_t)d1->cells[j] * (cc8typ_t)d2->cells[i];
    }
    carry /= DEC8_CELL_MASK;

    /* Step 2: the main body of the multiplication */
    i = MIN(d1->ncells + d2->ncells, DEC8_CELL_SIZE);
    n = i;
    while (i > 0) {
        j = MIN(i, (uint32)d2->ncells); // j < i and j < d2.ncells
        i--;
        while (j-- > 0 && (i - j) < (uint32)d1->ncells) {
            carry += (cc8typ_t)d1->cells[i - j] * (cc8typ_t)d2->cells[j];
        }
        rs->cells[i] = carry % (cc8typ_t)DEC8_CELL_MASK;
        carry /= DEC8_CELL_MASK;
    }

    rs->ncells = (uint8)n;
    rs->sign = d1->sign ^ d2->sign;
    rs->expn = d1->expn + d2->expn;

    /* Step 3: handle carry */
    if (carry > 0) {
        cm_dec8_rebuild(rs, (uint32)carry);
    }

    (void)cm_dec8_trim_zeros(rs);
    return GS_SUCCESS;
}

/**
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller, i.g. !DECIMAL_IS_ZERO(d)
 */
static inline status_t cm_dec8_init_inverse(const dec8_t *d, dec8_t *d_inv)
{
    return cm_real_to_dec8_inexac(1.0 / cm_dec8_to_real(d), d_inv);
}

/**
 * Computed the inverse of a decimal, inv_d = 1 / d
 * The Newton Inversion algorithm is used:
 * $x_{i+1} = 2x_{i} - dx^2_{i} = x_i(2-d * x_i)$
 */
static inline void cm_dec8_inv(const dec8_t *d, dec8_t *inv_d)
{
    uint32 i;
    dec8_t delta;

    // Step 1. compute an initial and approximate inverse by 1/double(dec)
    (void)cm_dec8_init_inverse(d, inv_d);
    DEC8_DEBUG_PRINT(inv_d, "inv_init_value");

    // Step 2. Newton iteration begins, At least 2 iterations are required
    for (i = 0; i <= 10; i++) {
        // set delta to x(1-d*x)
        (void)cm_dec8_mul_op(d, inv_d, &delta);          // set delta to d * inv_d
        (void)cm_dec8_sub_op(&DEC8_ONE, &delta, &delta); // set delta to 1 - delta
        (void)cm_dec8_mul_op(&delta, inv_d, &delta);     // set delta to delta * inv_d
        DEC8_DEBUG_PRINT(&delta, "inv delta: %u", i);

        (void)cm_dec8_add_op(inv_d, &delta, inv_d); // set inv_d(i) to inv_d(i) + delta
        DEC8_DEBUG_PRINT(inv_d, "inv(x): %u", i);

        if (cm_dec8_taylor_break(inv_d, &delta, MAX_NUM_CMP_PREC)) {
            break;
        }
    }
}

/**
 * The division of two decimals: dec1 / dec2
 */
status_t cm_dec8_divide(const dec8_t *dec1, const dec8_t *dec2, dec8_t *result)
{
    dec8_t inv_dec2;
    uint8 res_sign;

    if (DECIMAL_IS_ZERO(dec2)) {
        GS_THROW_ERROR(ERR_ZERO_DIVIDE);
        return GS_ERROR;
    }

    if (DECIMAL_IS_ZERO(dec1)) {
        cm_zero_dec8(result);
        return GS_SUCCESS;
    }

    if (cm_dec8_is_absolute_one(dec2)) {
        res_sign = dec1->sign ^ dec2->sign;
        cm_dec8_copy(result, dec1);
        result->sign = res_sign;
        return GS_SUCCESS;
    }

    cm_dec8_inv(dec2, &inv_dec2);

    return cm_dec8_multiply(dec1, &inv_dec2, result);
}

/**
 * Get a carry for rounding
 * + cpos -- the position of truncating in decimal->cells
 * + npos -- the position of truncating in decimal->cells[x]
 * @author , 2018/06/04
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller, i.g. npos<DEC_CELL_DIGIT
 */
static inline uint32 cm_dec8_make_round_carry(dec8_t *dec, round_mode_t rnd_mode, uint32 cpos, uint32 npos)
{
    switch (rnd_mode) {
        case ROUND_HALF_UP:
            return g_5ten_powers[DEC8_CELL_DIGIT - npos];
        case ROUND_TRUNC:
            return 0;
        default:
            CM_NEVER;
            return 0;
    }
}

/* d_pos: the round position in decimal */
/**
 * @note
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller, i.g. d_pos + DEC_CELL_DIGIT >= 0
 */
static inline bool32 cm_dec8_round_cells(dec8_t *dec, int32 precision, int32 d_pos, div_t *d, uint32 *carry,
    round_mode_t rnd_mode)
{
    int32 i;
    int32 r_pos;

    // d_pos + DEC_CELL_DIGIT is the round position in cells
    r_pos = d_pos + DEC8_CELL_DIGIT;

    if (r_pos > (int32)DEC8_MAX_ALLOWED_PREC) { // the rounded position exceeds the maximal precision
        return GS_FALSE;
    }
    // Step 1: round begin
    *d = div(r_pos, DEC8_CELL_DIGIT);

    if (d->quot >= dec->ncells) {
        return GS_TRUE;
    }

    dec->ncells = (uint8)(d->quot + 1);
    *carry = cm_dec8_make_round_carry(dec, rnd_mode, (uint32)d->quot, (uint32)d->rem);
    for (i = d->quot; i >= 0; --i) {
        *carry += dec->cells[i];
        if (*carry >= DEC8_CELL_MASK) {
            dec->cells[i] = *carry - DEC8_CELL_MASK;
            *carry = 1;
        } else {
            dec->cells[i] = *carry;
            *carry = 0;
            break;
        }
    }

    // Step 2. Check valid again
    if (i <= 0) { // the rounding mode may change the precision
        d_pos += (*carry == 0) ? (int32)cm_count_u32digits(dec->cells[0]) : (DEC8_CELL_DIGIT + 1);
        if (d_pos > precision) {
            return GS_FALSE;
        }
    }

    // Step 3. handle carry, truncate tailing digits to zeros
    dec->cells[d->quot] /= (c8typ_t)g_1ten_powers[DEC8_CELL_DIGIT - d->rem];
    dec->cells[d->quot] *= (c8typ_t)g_1ten_powers[DEC8_CELL_DIGIT - d->rem];

    return GS_TRUE;
}

/**
 * Round the number with fixed precision and scale, and return the number
 * of 10-base digits before the scale position
 */
static inline bool32 cm_dec8_round(dec8_t *dec, int32 precision, int32 scale, round_mode_t rnd_mode)
{
    div_t d; /* used for round */
    uint32 carry = 0;
    int32 r_pos;

    GS_RETVALUE_IFTRUE(DECIMAL_IS_ZERO(dec), GS_TRUE);

    scale += D8EXP_2_SEXP(dec->expn);
    r_pos = scale + (int32)cm_count_u32digits(dec->cells[0]); // r_pos is then the length of the scaled cells

    // Step 0: early check
    GS_RETVALUE_IFTRUE(r_pos > precision, GS_FALSE);

    if (r_pos < 0) {
        cm_zero_dec8(dec);
        return GS_TRUE;
    }

    // Step 1: scale round
    GS_RETVALUE_IFTRUE(!cm_dec8_round_cells(dec, precision, scale, &d, &carry, rnd_mode), GS_FALSE);

    // The cell[0] may be truncated to zero, e.g. 0.0043 for number(3,2)
    // Then zero is returned
    if ((d.quot == 0) && (carry == 0) && (dec->cells[0] == 0)) {
        cm_zero_dec8(dec);
        return GS_TRUE;
    }

    if (carry > 0) {
        cm_dec8_rebuild(dec, 1);
    }

    cm_dec8_trim_zeros(dec);
    return GS_TRUE;
}


/**
 * Adjust a decimal into fixed precision and scale. If failed, an error
 * will be returned.
 * @see cm_dec8_round
 */
status_t cm_adjust_dec8(dec8_t *dec, int32 precision, int32 scale)
{
    return cm_adjust_dec8_with_mode(dec, precision, scale, ROUND_HALF_UP); 
}

status_t cm_adjust_dec8_with_mode(dec8_t *dec, int32 precision, int32 scale, round_mode_t rnd_mode) {
    if (precision == GS_UNSPECIFIED_NUM_PREC) {
        return GS_SUCCESS;
    }

    if (!cm_dec8_round(dec, precision, scale, rnd_mode)) {
        GS_THROW_ERROR(ERR_VALUE_ERROR, "value larger than specified precision");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/**
 * Get the carry of a decimal with negative expn when convert decimal into integer
 * @note Required: dec->expn < 0
 * @author Added 2019/02/12
 */
static inline int32 dec8_make_negexpn_round_value(const dec8_t *dec, round_mode_t rnd_mode)
{
    switch (rnd_mode) {
        case ROUND_FLOOR:
            return DEC_IS_NEGATIVE(dec) ? -1 : 0;

        case ROUND_HALF_UP: {
            // e.g., 0.5 ==> 1, 0.499 ==> 0
            int32 val = ((dec->expn == -1) && (dec->cells[0] >= DEC8_HALF_MASK)) ? 1 : 0;
            return DEC_IS_NEGATIVE(dec) ? -val : val;
        }

        case ROUND_CEILING:
            return DEC_IS_NEGATIVE(dec) ? 0 : 1;

        case ROUND_TRUNC:
        default:
            return 0;
    }
}

/* * Round a positive and non-zero decimal into uint64 */
/* @author Added 2019/02/12 */
static inline uint64 dec8_make_negexpn_round_value2(const dec8_t *dec, round_mode_t rnd_mode)
{
    switch (rnd_mode) {
        case ROUND_HALF_UP:
            // e.g., 0.5 ==> 1, 0.499 ==> 0
            return ((dec->expn == -1) && (dec->cells[0] >= DEC8_HALF_MASK)) ? 1 : 0;

        case ROUND_CEILING:
            return 1;

        case ROUND_TRUNC:
        case ROUND_FLOOR:
        default:
            return 0;
    }
}

status_t cm_dec8_to_uint64(const dec8_t *dec, uint64 *u64, round_mode_t rnd_mode)
{
    if (DEC_IS_NEGATIVE(dec)) {
        GS_THROW_ERROR(ERR_VALUE_ERROR, "convert NUMBER into UINT64 failed");
        return GS_ERROR;
    }

    if (DECIMAL_IS_ZERO(dec)) {
        *u64 = 0;
        return GS_SUCCESS;
    }

    if (dec->expn < 0) {
        *u64 = dec8_make_negexpn_round_value2(dec, rnd_mode);
        return GS_SUCCESS;
    }

    // the maximal UINT64 is 1844 67440737 09551615
    if (dec->expn > 2 || (dec->expn == 2 && dec->cells[0] > 1844)) {
        GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "UINT64");
        return GS_ERROR;
    }

    uint32 i;
    uint64 u64h = dec->cells[0]; // the highest cell
    uint64 u64l = 0;             // the tailing cells

    for (i = 1; i <= (uint32)dec->expn && i < (uint32)dec->ncells; i++) {
        u64l = u64l * DEC8_CELL_MASK + dec->cells[i];
    }

    // here expn must be in [0, 2]
    u64h *= g_pow8_u64[(uint32)dec->expn];
    if (i <= (uint32)dec->expn) {
        u64l *= g_pow8_u64[(uint32)(dec->expn + 1) - i];
        i = dec->expn + 1;
    }

    // do round
    if (i < (uint32)dec->ncells) { // here i is dec->expn + 1
        switch (rnd_mode) {
            case ROUND_CEILING:
                u64l += DEC_IS_NEGATIVE(dec) ? 0 : 1;
                break;

            case ROUND_FLOOR:
                u64l += DEC_IS_NEGATIVE(dec) ? 1 : 0;
                break;

            case ROUND_HALF_UP:
                u64l += (dec->cells[i] >= DEC8_HALF_MASK) ? 1 : 0;
                break;

            case ROUND_TRUNC:
            default:
                break;
        }
    }

    // overflow check
    if (u64h == 18440000000000000000ull && u64l > 6744073709551615ull) {
        GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "UINT64");
        return GS_ERROR;
    }

    *u64 = u64h + u64l;

    return GS_SUCCESS;
}

status_t cm_dec8_to_int64(const dec8_t *dec, int64 *val, round_mode_t rnd_mode)
{
    CM_POINTER(dec);

    if (DECIMAL_IS_ZERO(dec)) {
        *val = 0;
        return GS_SUCCESS;
    }

    if (dec->expn < 0) {
        *val = dec8_make_negexpn_round_value(dec, rnd_mode);
        return GS_SUCCESS;
    }

    // the maximal BIGINT is 922 33720368 54775807
    if (dec->expn > 2 || (dec->expn == 2 && dec->cells[0] > 922)) {
        GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "BIGINT");
        return GS_ERROR;
    }

    uint32 i;
    uint64 u64_val = dec->cells[0];

    for (i = 1; i <= (uint32)dec->expn && i < (uint32)dec->ncells; i++) {
        u64_val = u64_val * DEC8_CELL_MASK + dec->cells[i];
    }
    if (i <= (uint32)dec->expn) {
        u64_val *= g_pow8_u64[(uint32)(dec->expn + 1) - i];
        i = dec->expn + 1;
    }

    // do round
    if (i < (uint32)dec->ncells) { // here i is equal to dec->expn + 1
        switch (rnd_mode) {
            case ROUND_CEILING:
                u64_val += DEC_IS_NEGATIVE(dec) ? 0 : 1;
                break;

            case ROUND_FLOOR:
                u64_val += DEC_IS_NEGATIVE(dec) ? 1 : 0;
                break;

            case ROUND_HALF_UP:
                u64_val += (dec->cells[i] >= DEC8_HALF_MASK) ? 1 : 0;
                break;

            case ROUND_TRUNC:
            default:
                break;
        }
    }

    // overflow check
    if (u64_val > 9223372036854775807ull) {
        if (DEC_IS_NEGATIVE(dec) && u64_val == 9223372036854775808ull) {
            *val = GS_MIN_INT64;
            return GS_SUCCESS;
        }

        GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "BIGINT");
        return GS_ERROR;
    }

    *val = DEC_IS_NEGATIVE(dec) ? -(int64)u64_val : (int64)u64_val;

    return GS_SUCCESS;
}

/**
 * Convert a decimal into bigint/int64. if overflow happened, the border
 * value is returned.
 * @return
 * 0: -- no overflow and underflow
 * 1: -- overflow, and return GS_MAX_INT64
 * -1: -- underflow, and return GS_MIN_INT64
 * @author Added, 2018/06/17
 */
int32 cm_dec8_to_int64_range(const dec8_t *dec, int64 *i64, round_mode_t rnd_mode)
{
    if (cm_dec8_to_int64(dec, i64, rnd_mode) != GS_SUCCESS) {
        cm_reset_error();
        *i64 = DEC_IS_NEGATIVE(dec) ? GS_MIN_INT64 : GS_MAX_INT64;
        return DEC_IS_NEGATIVE(dec) ? -1 : 1;
    }

    return 0;
}

/**
 * Convert a decimal into uint32. if overflow happened, return ERROR
 */
status_t cm_dec8_to_uint32(const dec8_t *dec, uint32 *i32, round_mode_t rnd_mode)
{
    if (DECIMAL_IS_ZERO(dec)) {
        *i32 = 0;
        return GS_SUCCESS;
    }

    // the maximal UINT32 42 9496 7295
    if (dec->expn > 2 || DEC_IS_NEGATIVE(dec)) {
        GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "UNSIGNED INTEGER");
        return GS_ERROR;
    }

    if (dec->expn < 0) {
        *i32 = (uint32)dec8_make_negexpn_round_value(dec, rnd_mode);
        return GS_SUCCESS;
    }

    uint32 i;
    uint64 u64_val = dec->cells[0];
    for (i = 1; i <= (uint32)dec->expn && i < (uint32)dec->ncells; i++) {
        u64_val = u64_val * DEC8_CELL_MASK + dec->cells[i];
    }

    while (i <= (uint32)dec->expn) {
        u64_val *= DEC8_CELL_MASK;
        ++i;
    }

    if (i < (uint32)dec->ncells) {
        switch (rnd_mode) {
            case ROUND_CEILING:
                u64_val += 1;
                break;

            case ROUND_HALF_UP:
                u64_val += (dec->cells[i] >= DEC8_HALF_MASK) ? 1 : 0;
                break;

            case ROUND_FLOOR:
            case ROUND_TRUNC:
            default:
                break;
        }
    }

    TO_UINT32_OVERFLOW_CHECK(u64_val, uint64);

    *i32 = (uint32)u64_val;
    return GS_SUCCESS;
}


/**
 * Convert a decimal into int32. if overflow happened, return ERROR
 */
status_t cm_dec8_to_int32(const dec8_t *dec, int32 *i32, round_mode_t rnd_mode)
{
    if (DECIMAL_IS_ZERO(dec)) {
        *i32 = 0;
        return GS_SUCCESS;
    }

    if (dec->expn < 0) {
        *i32 = dec8_make_negexpn_round_value(dec, rnd_mode);
        return GS_SUCCESS;
    }

    // the maximal INTEGER 21 47483648
    if (dec->expn > 1) {
        GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "INTEGER");
        return GS_ERROR;
    }

    uint32 i = 1;
    int64 i64_val = dec->cells[0];

    if (dec->expn == 1) {
        i = 2;
        i64_val *= DEC8_CELL_MASK;
        if (dec->ncells > 1) {
            i64_val += dec->cells[1];
        }
    }

    if (i < (uint32)dec->ncells) { // here i is equal to dec->expn + 1
        switch (rnd_mode) {
            case ROUND_CEILING:
                i64_val += DEC_IS_NEGATIVE(dec) ? 0 : 1;
                break;

            case ROUND_FLOOR:
                i64_val += DEC_IS_NEGATIVE(dec) ? 1 : 0;
                break;

            case ROUND_HALF_UP:
                i64_val += (dec->cells[i] >= DEC8_HALF_MASK) ? 1 : 0;
                break;

            case ROUND_TRUNC:
            default:
                break;
        }
    }

    if (DEC_IS_NEGATIVE(dec)) {
        i64_val = -i64_val;
    }

    INT32_OVERFLOW_CHECK(i64_val);

    *i32 = (int32)i64_val;
    return GS_SUCCESS;
}

/**
 * Convert a decimal into integer(<= MAX_INT32) with max_prec (integer part, <= 10).
 * if overflow happened, the border value is returned.
 * @return
 * 0: -- no overflow and underflow
 * 1: -- overflow, and return GS_MAX_INT32
 * -1: -- underflow, and return GS_MIN_INT32
 */
static inline int32 cm_dec8_to_int32_range(const dec8_t *dec, int32 *val, uint32 max_prec, round_mode_t rnd_mode)
{
    if (cm_dec8_to_int32(dec, val, rnd_mode) != GS_SUCCESS) {
        cm_reset_error();
        *val = DEC_IS_NEGATIVE(dec) ? GS_MIN_INT32 : GS_MAX_INT32;
        return DEC_IS_NEGATIVE(dec) ? -1 : 1;
    }

    return 0;
}

/**
 * To decide whether a decimal is an integer
 * @author Added 2018/06/04
 */
bool32 cm_dec8_is_integer(const dec8_t *dec)
{
    uint32 i;

    if (DECIMAL_IS_ZERO(dec)) {
        return GS_TRUE;
    }

    if (dec->expn < 0) {
        return GS_FALSE;
    }

    i = dec->expn + 1;
    for (; i < (uint32)dec->ncells; i++) {
        if (dec->cells[i] > 0) {
            return GS_FALSE;
        }
    }
    return GS_TRUE;
}

/**
 * To decide whether a integer decimal is odd;
 * note that the decimal must be an integer
 * @author Added 2018/06/04
 */
static inline bool32 cm_dec8_is_odd(const dec8_t *integer)
{
    if (DECIMAL_IS_ZERO(integer)) {
        return GS_FALSE;
    }
    return integer->cells[(int32)integer->expn] & 1;
}

status_t cm_dec8_floor(dec8_t *dec)
{
    uint32 i;
    bool32 has_tail = GS_FALSE;
    uint32 carry;

    GS_RETVALUE_IFTRUE(DECIMAL_IS_ZERO(dec), GS_SUCCESS);

    if (dec->expn < 0) {
        if (DEC_IS_NEGATIVE(dec)) {
            cm_dec8_copy(dec, &DEC8_ONE);
            dec->sign = DEC_SIGN_MINUS;
        } else {
            cm_zero_dec8(dec);
        }
        return GS_SUCCESS;
    }

    i = (dec->expn / SEXP_2_D8EXP(DEC8_CELL_DIGIT)) + 1;
    for (; i < (uint32)dec->ncells; i++) {
        if (dec->cells[i] > 0) {
            has_tail = GS_TRUE;
            break;
        }
    }

    GS_RETVALUE_IFTRUE(!has_tail, GS_SUCCESS);

    dec->ncells = dec->expn + 1;
    if (!DEC_IS_NEGATIVE(dec) && has_tail) {
        cm_dec8_trim_zeros(dec);
        return GS_SUCCESS;
    }

    carry = 1;
    i = dec->ncells;
    while (i-- > 0) {
        dec->cells[i] += carry;
        carry = (dec->cells[i] >= DEC8_CELL_MASK);
        if (carry == 0) {
            break;
        }

        dec->cells[i] = 0;
    }
    if (carry > 0) {
        cm_dec8_rebuild(dec, 1);
        DEC8_OVERFLOW_CHECK(dec);
    }

    cm_dec8_trim_zeros(dec);
    return GS_SUCCESS;
}

status_t cm_dec8_ceil(dec8_t *dec)
{
    uint32 i;
    bool32 has_tail = GS_FALSE;
    uint32 carry;

    GS_RETVALUE_IFTRUE(DECIMAL_IS_ZERO(dec), GS_SUCCESS);

    if (dec->expn < 0) {
        if (DEC_IS_NEGATIVE(dec)) {
            cm_zero_dec8(dec);
        } else {
            cm_dec8_copy(dec, &DEC8_ONE);
        }
        return GS_SUCCESS;
    }

    i = (dec->expn / SEXP_2_D8EXP(DEC8_CELL_DIGIT)) + 1;
    for (; i < (uint32)dec->ncells; i++) {
        if (dec->cells[i] > 0) {
            has_tail = GS_TRUE;
            break;
        }
    }

    GS_RETVALUE_IFTRUE(!has_tail, GS_SUCCESS);

    dec->ncells = dec->expn + 1;
    if (DEC_IS_NEGATIVE(dec) && has_tail) {
        cm_dec8_trim_zeros(dec);
        return GS_SUCCESS;
    }

    carry = 1;
    i = dec->ncells;
    while (i-- > 0) {
        dec->cells[i] += carry;
        carry = (dec->cells[i] >= DEC8_CELL_MASK);
        if (carry == 0) {
            break;
        }

        dec->cells[i] = 0;
    }
    if (carry > 0) {
        cm_dec8_rebuild(dec, 1);
        DEC8_OVERFLOW_CHECK(dec);
    }

    cm_dec8_trim_zeros(dec);
    return GS_SUCCESS;
}

/* Round a decimal by persevering at most scale digits after decimal point
 * The round mode can only be ROUND_HALF_UP or ROUND_TRUNC
 * Performance sensitivity.CM_ASSERT should be guaranteed by caller,
 * i.g. rnd_mode == ROUND_HALF_UP || rnd_mode == ROUND_TRUNC
 */
status_t cm_dec8_scale(dec8_t *dec, int32 scale, round_mode_t rnd_mode)
{
    int32 i, r_pos, cpos;
    uint32 carry = 0;
    uint32 npos;

    GS_RETVALUE_IFTRUE(DECIMAL_IS_ZERO(dec), GS_SUCCESS);

    r_pos = D8EXP_2_SEXP(dec->expn) + DEC8_CELL_DIGIT + scale;
    if (r_pos < 0) {
        cm_zero_dec8(dec);
        return GS_SUCCESS;
    }
    // the rounded position exceeds the maximal precision
    GS_RETVALUE_IFTRUE((r_pos > DEC8_MAX_ALLOWED_PREC), GS_SUCCESS);

    cpos = r_pos / DEC8_CELL_DIGIT;
    npos = ((uint32)r_pos % DEC8_CELL_DIGIT);

    if ((uint32)cpos >= dec->ncells) {
        return GS_SUCCESS;
    }

    npos = DEC8_CELL_DIGIT - npos;
    if (rnd_mode == ROUND_HALF_UP) {
        carry = g_5ten_powers[npos];
        for (i = cpos; i >= 0; --i) {
            dec->cells[i] += carry;
            carry = (dec->cells[i] >= DEC8_CELL_MASK);
            if (!carry) {
                break;
            }
            dec->cells[i] -= DEC8_CELL_MASK;
        }
    }

    dec->cells[cpos] /= g_1ten_powers[npos];
    dec->cells[cpos] *= g_1ten_powers[npos];

    // trimming zeros and recompute the dec->ncells
    while ((cpos >= 0) && (dec->cells[cpos] == 0)) {
        --cpos;
    }
    dec->ncells = (uint8)(cpos + 1);

    if (carry) {
        cm_dec8_rebuild(dec, 1);
        DEC8_OVERFLOW_CHECK(dec);
    }
    cm_dec8_trim_zeros(dec);
    return GS_SUCCESS;
}

/* decimal 3/8 = 0.375 */
static const dec8_t DEC8_3_in_8 = {
    .expn = SEXP_2_D8EXP(-DEC8_CELL_DIGIT),
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)1,
    .cells = { 37500000 }
};

/* decimal 10/8 = 1.25 */
static const dec8_t DEC8_10_in_8 = {
    .expn = (int8)0,
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)2,
    .cells = { 1, 25000000 }
};

/* decimal 15/8 = 1.875 */
static const dec8_t DEC8_15_in_8 = {
    .expn = (int8)0,
    .sign = DEC_SIGN_PLUS,
    .ncells = (uint8)2,
    .cells = { 1, 87500000 }
};

status_t cm_dec8_sqrt(const dec8_t *d, dec8_t *r)
{
    dec8_t ti, yi; // auxiliary variable

    if (DECIMAL_IS_ZERO(d)) {
        cm_zero_dec8(r);
        return GS_SUCCESS;
    }

    if (DEC_IS_NEGATIVE(d)) {
        GS_THROW_ERROR(ERR_VALUE_ERROR, "argument value of function sqrt must not be a negative number");
        return GS_ERROR;
    }

    /* The Halley's algorithm is applied to compute the square roots.
     * (0). T(i) = d * X(i)
     * (1). Y(i) = T(i) * X(i)
     * (2). X(i+1) = X(i)/8 * {15 - Y(i) * (10 - 3 * Y(i))} = X(i) * K
     * (3). r = d * X(i + 1) = T(i) * K
     * This algorithm has better performance than Newton's method,
     * (0). X(i+1) = 0.5 * (X(i) + d/X(i))
     * which involves division, multiplication and addition.
     * Set an initial value */
    (void)cm_real_to_dec8_inexac(sqrt(1.0 / cm_dec8_to_real(d)), r); /* set r to 1.0/sqrt(d) */

    (void)cm_dec8_mul_op(d, r, &ti);   /* set ti to d * r */
    (void)cm_dec8_mul_op(&ti, r, &yi); /* set yi to yi * r */

    (void)cm_dec8_mul_op(&yi, &DEC8_3_in_8, r); /* set r to 3/8 * yi */
    (void)cm_dec8_sub_op(&DEC8_10_in_8, r, r);  /* set r to 10/8 - r */
    (void)cm_dec8_mul_op(&yi, r, r);            /* set r to r * yi */

    (void)cm_dec8_sub_op(&DEC8_15_in_8, r, r); /* set r to 15/8 - r */
    (void)cm_dec8_mul_op(&ti, r, r);           /* set r to r * ti */

    return cm_dec8_finalise(r, MAX_NUMERIC_BUFF, GS_FALSE);
}

/**
 * Compute the sin(x) using Taylor series, where x in (0, pi/4)
 * @author Added 2018/09/11
 * sin x = x-x^3/3!+x^5/5!-����+(-1)^(n)*(x^(2n+1))/(2n+1)!+����
 */
static status_t cm_dec8_sin_frac(const dec8_t *x, dec8_t *sin_x)
{
    dec8_t x_pow2;
    dec8_t x_i;
    dec8_t item;

    /* initialize the iteration variables */
    (void)cm_dec8_mul_op(x, x, &x_pow2); // set x_pow2 to x * x
    cm_dec8_copy(sin_x, x);              // set sin(x) to x
    cm_dec8_copy(&x_i, x);               // set x(i) to x

    for (uint32 i = _I(3); i < ELEMENT_COUNT(g_dec8_inv_fact); i += 2) {
        (void)cm_dec8_mul_op(&x_i, &x_pow2, &x_i); // set x(i) to x^2 * x(i-1)
        (void)cm_dec8_mul_op(&x_i, &g_dec8_inv_fact[i], &item);
        DEC8_DEBUG_PRINT(&item, "The item at [%u]", i >> 1);

        if (i & 2) {
            (void)cm_dec8_add_op(sin_x, &item, sin_x);
        } else {
            (void)cm_dec8_sub_op(sin_x, &item, sin_x);
        }
        DEC8_DEBUG_PRINT(sin_x, "The %u-th iteration", i >> 1);
        if (cm_dec8_taylor_break(sin_x, &item, MAX_NUM_CMP_PREC)) {
            break;
        }
    }

    return GS_SUCCESS;
}

/**
 * Compute the cos(x) using Taylor series, where x in (0, pi/4)
 * @author Added 2018/09/13
 * cos x = 1-x^2/2!+x^4/4!-����+(-1)^(n)*(x^(2n))/(2n)!+����
 */
static status_t cm_dec8_cos_frac(const dec8_t *x, dec8_t *cos_x)
{
    dec8_t x_pow2;
    dec8_t x_i;
    dec8_t item;

    (void)cm_dec8_mul_op(x, x, &x_pow2);
    cm_dec8_copy(&x_i, &x_pow2);

    // 1 - (x^2)/2
    (void)cm_dec8_mul_op(&x_pow2, &DEC8_HALF_ONE, &item);
    (void)cm_dec8_sub_op(&DEC8_ONE, &item, cos_x);

    for (uint32 i = _I(4); i < ELEMENT_COUNT(g_dec8_inv_fact); i += 2) {
        (void)cm_dec8_mul_op(&x_i, &x_pow2, &x_i); // set x(i) to x^2 * x(i-1)
        (void)cm_dec8_mul_op(&x_i, &g_dec8_inv_fact[i], &item);
        DEC8_DEBUG_PRINT(&item, "The item at [%u]", i >> 1);

        if (i & 2) {
            (void)cm_dec8_sub_op(cos_x, &item, cos_x);
        } else {
            (void)cm_dec8_add_op(cos_x, &item, cos_x);
        }
        DEC8_DEBUG_PRINT(cos_x, "The %u-th iteration", i >> 1);
        if (cm_dec8_taylor_break(cos_x, &item, MAX_NUM_CMP_PREC)) {
            break;
        }
    }

    return GS_SUCCESS;
}

#define MAX8_RANGE_PREC (MAX_NUM_CMP_PREC - DEC8_CELL_DIGIT)

static inline bool32 cm_dec8_range_to_2pi(const dec8_t *x, dec8_t *y, double *dy)
{
    static const double __2pi = GS_PI * 2.0;
    bool32 is_neg = GS_FALSE;

    *y = *x;
    if (DEC_IS_NEGATIVE(x)) {
        y->sign = DEC_SIGN_PLUS;
        is_neg = GS_TRUE;
    }

    dec8_t rem;
    int32 scale;
    do {
        *dy = cm_dec8_to_real(y);
        if (*dy < __2pi) {
            break;
        }

        (void)cm_dec8_mul_op(&DEC8_INV_2PI, y, &rem); // set rem to y /(2pi)

        scale = (rem.expn <= SEXP_2_D8EXP(MAX8_RANGE_PREC)) ? 0 : (MAX8_RANGE_PREC)-D8EXP_2_SEXP(rem.expn);

        (void)cm_dec8_scale(&rem, scale, ROUND_TRUNC); // truncate rem to integer
        (void)cm_dec8_mul_op(&rem, &DEC8_2PI, &rem);
        (void)cm_dec8_sub_op(y, &rem, y);
    } while (1);

    return is_neg;
}

static status_t cm_dec8_sin_op(const dec8_t *x, dec8_t *sin_x)
{
    dec8_t tx;
    double dx;
    bool32 is_neg = cm_dec8_range_to_2pi(x, &tx, &dx);

    if (dx < GS_PI_2) { // [0, pi/2)
        // do nothing
    } else if (dx < GS_PI) {                      // [pi/2, pi)
        (void)cm_dec8_sub_op(&DEC8_PI, &tx, &tx); // pi - tx
    } else if (dx < GS_PI_2 + GS_PI) {            // [PI, 3/2pi)
        (void)cm_dec8_sub_op(&tx, &DEC8_PI, &tx); // tx - pi
        is_neg = !is_neg;
    } else {
        (void)cm_dec8_sub_op(&DEC8_2PI, &tx, &tx); // 2pi - tx
        is_neg = !is_neg;
    }

    dx = cm_dec8_to_real(&tx);
    if (dx < GS_PI_4) {
        (void)cm_dec8_sin_frac(&tx, sin_x);
    } else {
        (void)cm_dec8_sub_op(&DEC8_HALF_PI, &tx, &tx);
        (void)cm_dec8_cos_frac(&tx, sin_x);
    }
    if (is_neg) {
        sin_x->sign = DEC_SIGN_MINUS;
    }
    return GS_SUCCESS;
}

/**
 * Compute the sin(x) using Taylor series
 * @author Added 2018/07/04
 * sin x = x-x^3/3!+x^5/5!-����+(-1)^(n)*(x^(2n+1))/(2n+1)!+����
 */
status_t cm_dec8_sin(const dec8_t *dec, dec8_t *result)
{
    if (DECIMAL_IS_ZERO(dec)) {
        cm_zero_dec8(result);
        return GS_SUCCESS;
    }

    (void)cm_dec8_sin_op(dec, result);
    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

/**
 * Compute the cos(x) using cos(x) = sin (x +pi/2)
 * @author Added 2018/07/04
 */
status_t cm_dec8_cos(const dec8_t *dec, dec8_t *result)
{
    dec8_t tmp_dec;

    if (DECIMAL_IS_ZERO(dec)) {
        cm_dec8_copy(result, &DEC8_ONE);
        return GS_SUCCESS;
    }

    (void)cm_dec8_add_op(dec, &DEC8_HALF_PI, &tmp_dec);

    return cm_dec8_sin(&tmp_dec, result);
}

/**
 * Compute the tan(x) using tan(x) = sin(x) / cos(x)
 * @author Added 2019/07/09
 */
status_t cm_dec8_tan(const dec8_t *dec, dec8_t *result)
{
    dec8_t sin_dec;
    dec8_t cos_dec;
    dec8_t inv_cos_dec;

    if (DECIMAL_IS_ZERO(dec)) {
        cm_zero_dec8(result);
        return GS_SUCCESS;
    }

    (void)cm_dec8_sin(dec, &sin_dec); // sin(x)
    (void)cm_dec8_cos(dec, &cos_dec); // cos(x)
    if (DECIMAL_IS_ZERO(&cos_dec)) {
        GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "the value is not exist");
        return GS_ERROR;
    }
    cm_dec8_inv(&cos_dec, &inv_cos_dec);                  // 1 / cos(x)
    (void)cm_dec8_mul_op(&sin_dec, &inv_cos_dec, result); // sin(x) / cos(x)
    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

/**
 * Compute the asin(x) using Newton's method
 * @author Added 2018/07/12
 * Xn+1 = Xn - f(Xn)/f'(Xn) = Xn - (sin(Xn) - a)/cos(Xn)
 */
status_t cm_dec8_asin(const dec8_t *d, dec8_t *rs)
{
    dec8_t last_dec;
    dec8_t tmp_dec;
    dec8_t cos_dec;
    dec8_t inv_cos_dec;
    dec8_t initial_dec;
    double initial_val;

    if (DECIMAL_IS_ZERO(d)) {
        cm_zero_dec8(rs);
        return GS_SUCCESS;
    }

    if (dec8_is_greater_than_one(d)) {
        GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "argument value of function ASIN must between [-1, 1]");
        return GS_ERROR;
    }

    if (cm_dec8_is_absolute_one(d)) {
        cm_dec8_copy(rs, &DEC8_HALF_PI);
        rs->sign = d->sign;
        return GS_SUCCESS;
    }

    // result : Xn
    cm_dec8_copy(&initial_dec, d);
    initial_val = asin(cm_dec8_to_real(d));
    if (GS_SUCCESS != cm_real_to_dec8(initial_val, rs)) {
        cm_dec8_copy(rs, d);
    }
    DEC8_DEBUG_PRINT(rs, "asin initial value");
    initial_dec.sign = d->sign; // dec : a

    do {
        cm_dec8_copy(&last_dec, rs);

        (void)cm_dec8_sin(rs, &tmp_dec); // sin(Xn)

        (void)cm_dec8_sub_op(&tmp_dec, &initial_dec, &tmp_dec); // sin(Xn) - a

        (void)cm_dec8_cos(rs, &cos_dec);     // cos(Xn)
        cm_dec8_inv(&cos_dec, &inv_cos_dec); // 1 / cos(Xn)

        (void)cm_dec8_mul_op(&tmp_dec, &inv_cos_dec, &tmp_dec); // (sin(Xn) - a) / cos(Xn)

        (void)cm_dec8_sub_op(rs, &tmp_dec, rs); // Xn -  (sin(Xn) - a) / cos(Xn)
        DEC8_DEBUG_PRINT(rs, "asin iteration");
    } while (!cm_dec8_taylor_break(rs, &tmp_dec, MAX_NUM_CMP_PREC));

    return cm_dec8_finalise(rs, MAX_NUMERIC_BUFF, GS_FALSE);
}

status_t cm_dec8_acos(const dec8_t *dec, dec8_t *result)
{
    dec8_t initial_dec;
    cm_dec8_copy(&initial_dec, dec);
    initial_dec.sign = DEC_SIGN_PLUS;
    if (dec8_is_greater_than_one(dec)) {
        GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "argument value of function ACOS must between [-1, 1]");
        return GS_ERROR;
    }
    (void)cm_dec8_asin(dec, result);
    (void)cm_dec8_sub_op(&DEC8_HALF_PI, result, result); // set acosx to PI - asinx
    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}


/**
 * Compute the tan(x) using atan(x)=asin(x/sqrt(x^2+1))
 */
status_t cm_dec8_atan(const dec8_t *dec, dec8_t *result)
{
    dec8_t tmp_x;
    dec8_t tmp_result;
    int sci_exp = DEC8_GET_SEXP(dec);

    if (DECIMAL_IS_ZERO(dec)) {
        cm_dec8_copy(result, &DEC8_ZERO);
        return GS_SUCCESS;
    }

    if (sci_exp > 63) { // when sci_exp>63, set atan(x)=+/-PI/2
        cm_dec8_copy(result, &DEC8_HALF_PI);
        result->sign = dec->sign;
    } else if (sci_exp < -63) { // when sci_exp<-63,set atan(x) to x
        cm_dec8_copy(result, dec);
    } else {
        (void)cm_dec8_mul_op(dec, dec, &tmp_x);              // x^2
        (void)cm_dec8_add_op(&tmp_x, &DEC8_ONE, &tmp_x);     // x^2 + 1
        (void)cm_dec8_sqrt(&tmp_x, &tmp_result);             // sqrt(x^2+1)
        (void)cm_dec8_divide(dec, &tmp_result, &tmp_result); // x/sqt(x^2+1)
        (void)cm_dec8_asin(&tmp_result, result);             // set atan(x) to asin(x/sqrt(x^2+1))
    }
    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

/**
 * arctan(y/x)    x>0
 * arctan(y/x)+PI y>=0,x<0
 * arctan(y/x)-PI y<0,x<0
 * Compute the tan(x) using atan2(y,x)= PI/2          y>0,x=0
 * -PI/2          y<0,x=0
 * undefined      y=0,x=0
*  */
status_t cm_dec8_atan2(const dec8_t *dec1, const dec8_t *dec2, dec8_t *result)
{
    dec8_t tmp_dec;
    dec8_t tmp_result;

    if (DECIMAL_IS_ZERO(dec1) && DECIMAL_IS_ZERO(dec2)) {
        GS_THROW_ERROR(ERR_NUM_OVERFLOW);
        return GS_ERROR;
    }

    if (DECIMAL_IS_ZERO(dec2)) {
        cm_dec8_copy(result, &DEC8_HALF_PI);
        result->sign = dec1->sign;
        return GS_SUCCESS;
    }

    (void)cm_dec8_divide(dec1, dec2, &tmp_dec); // set tmp_dec to dec1/dec2
    (void)cm_dec8_atan(&tmp_dec, &tmp_result);  // set tmp_result to atan(tmp_dec)

    if (dec2->sign == DEC_SIGN_PLUS) {
        cm_dec8_copy(result, &tmp_result);
    } else if (dec1->sign == DEC_SIGN_PLUS) {
        (void)cm_dec8_add_op(&tmp_result, &DEC8_PI, result);
    } else {
        (void)cm_dec8_sub_op(&tmp_result, &DEC8_PI, result);
    }

    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

status_t cm_dec8_tanh(const dec8_t *dec, dec8_t *result)
{
    dec8_t tmp_dec;
    dec8_t tmp_exp1;
    dec8_t tmp_exp2;
    dec8_t tmp_value1;
    dec8_t tmp_value2;

    cm_dec8_copy(&tmp_dec, dec);

    if (DECIMAL_IS_ZERO(dec)) {
        cm_dec8_copy(result, &DEC8_ZERO);
        return GS_SUCCESS;
    }

    int32 int_dec;
    (void)cm_dec8_to_int32_range(dec, &int_dec, 3, ROUND_HALF_UP); // Number 3 is the max accuracy of the integer part

    if (int_dec >= 296) { // overflow in numerical calculation
        cm_dec8_copy(result, &DEC8_ONE);
        return GS_SUCCESS;
    }
    if (int_dec <= -293) { // overflow in numerical calculation
        cm_dec8_copy(result, &DEC8_NEG_ONE);
        return GS_SUCCESS;
    }

    (void)cm_dec8_exp(dec, &tmp_exp1);                       // e^x
    (void)cm_dec8_divide(&DEC8_ONE, &tmp_exp1, &tmp_exp2);   // e^-x
    (void)cm_dec8_sub_op(&tmp_exp1, &tmp_exp2, &tmp_value1); // e^x-e^-x
    (void)cm_dec8_add_op(&tmp_exp1, &tmp_exp2, &tmp_value2); // e^x+e^-x
    (void)cm_dec8_divide(&tmp_value1, &tmp_value2, result);  // (e^x-e^-x)/(e^x+e^-x)

    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

/* x = exp(n), n is an integer */
static void cm_dec8_exp_n(int32 i32, dec8_t *x)
{
    dec8_t y;

    if (i32 < 0) {
        cm_dec8_exp_n(-i32, &y);
        cm_dec8_inv(&y, x);
        DEC8_DEBUG_PRINT(x, "exp(-n)");
        return;
    }

    if (i32 == 0) {
        cm_dec8_copy(x, &DEC8_ONE);
        return;
    }
    if (i32 == 1) {
        cm_dec8_copy(x, &DEC8_EXP);
        return;
    }

    cm_dec8_exp_n(i32 / 2, &y);
    (void)cm_dec8_mul_op(&y, &y, x);

    if (i32 & 1) {
        (void)cm_dec8_mul_op(x, &DEC8_EXP, x);
    }
    DEC8_DEBUG_PRINT(x, "exp(%d)", i32);
}

/* Compute the exp(x) using Taylor series, where abs(x) <= 0.5
 * @author Added by 2018/07/31
 *  */
static inline void cm_dec8_exp_frac(const dec8_t *x, dec8_t *exp_x, uint32 prec)
{
    uint32 i;
    dec8_t ni, x_i;

    // set exp(x) to 1 + x + x^2/2
    (void)cm_dec8_mul_op(x, x, &x_i);
    (void)cm_dec8_mul_op(&x_i, &DEC8_HALF_ONE, exp_x); // set exp_x to x^2/2

    (void)cm_dec8_add_op(exp_x, &DEC8_ONE, exp_x); // set exp_x to exp_x + 1
    (void)cm_dec8_add_op(exp_x, x, exp_x);         // set exp_x to exp_x + x

    for (i = _I(3); i < ELEMENT_COUNT(g_dec8_inv_fact); i++) {
        (void)cm_dec8_mul_op(&x_i, x, &x_i);                  // set xi to xi * x
        (void)cm_dec8_mul_op(&x_i, &g_dec8_inv_fact[i], &ni); // set ni to xi / (i!)

        DEC8_DEBUG_PRINT(&ni, "exp frac item: %u", i);

        (void)cm_dec8_add_op(exp_x, &ni, exp_x); // set exp_x to exp_x + ni
        DEC8_DEBUG_PRINT(exp_x, "exp frac iteration: %u", i);

        if (cm_dec8_taylor_break(exp_x, &ni, MAX_NUM_CMP_PREC)) {
            break;
        }
    }
}

static inline status_t cm_dec8_exp_op(const dec8_t *x, dec8_t *y, uint32 prec)
{
    if (DECIMAL_IS_ZERO(x)) {
        cm_dec8_copy(y, &DEC8_ONE);
        return GS_SUCCESS;
    }

    int32 int_x;

    (void)cm_dec8_to_int32_range(x, &int_x, 3, ROUND_HALF_UP);
    if (int_x >= 296) { // whether exp(296) * exp(-0.5) is greater than 10^128
        GS_THROW_ERROR(ERR_NUM_OVERFLOW);
        return GS_ERROR;
    } else if (int_x <= -293) {
        cm_zero_dec8(y);
        return GS_SUCCESS;
    }

    if (cm_dec8_is_integer(x)) {
        cm_dec8_exp_n(int_x, y);
        return GS_SUCCESS;
    }

    if (int_x == 0) { // whether [x] is equal to 0
        DEC8_DEBUG_PRINT(x, "exp(x), integer is zero");
        cm_dec8_exp_frac(x, y, prec);
        return GS_SUCCESS;
    }

    dec8_t frac_x;
    cm_int32_to_dec8(int_x, y);
    (void)cm_dec8_sub_op(x, y, &frac_x); // when frac_x <= 0.5, set frac_x to x - [x]
    DEC8_DEBUG_PRINT(y, "exp(integer)");
    DEC8_DEBUG_PRINT(&frac_x, "exp(frac)");
    cm_dec8_exp_frac(&frac_x, y, prec); // set y to exp(x - [x])

    cm_dec8_exp_n(int_x, &frac_x);       // set frac_x to exp([x])
    (void)cm_dec8_mul_op(y, &frac_x, y); // set y to y * frac_x

    return GS_SUCCESS;
}

status_t cm_dec8_exp(const dec8_t *dec, dec8_t *result)
{
    DEC8_DEBUG_PRINT(dec, "go into exp(x)");
    if (cm_dec8_exp_op(dec, result, MAX_NUM_CMP_PREC) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

/* Natural logarithm of small decimal
 * set y to ln(x), x <= 999999999 */
static inline void cm_dec8_ln_small(const dec8_t *x, dec8_t *ln_x, uint32 prec)
{
    uint32 i;
    dec8_t x4times;
    dec8_t delta, z2; /* two intermediate variables */

    (void)cm_real_to_dec8_inexac(log(cm_dec8_to_real(x)), ln_x);
    DEC8_DEBUG_PRINT(ln_x, "ln initial value");
    (void)cm_dec8_mul_op(x, &DEC8_FOUR, &x4times);
    for (i = 0; i <= 5; ++i) {
        // set delta to 4x/(x + exp(ln_x)) - 2
        (void)cm_dec8_exp_op(ln_x, &delta, prec);
        (void)cm_dec8_add_op(x, &delta, &z2);
        cm_dec8_inv(&z2, &delta);
        (void)cm_dec8_mul_op(&delta, &x4times, &z2);
        (void)cm_dec8_sub_op(&z2, &DEC8_TWO, &delta);
        DEC8_DEBUG_PRINT(&delta, "ln delta: %u", i);

        (void)cm_dec8_add_op(ln_x, &delta, ln_x);
        DEC8_DEBUG_PRINT(ln_x, "ln iteration: %u", i);

        if (cm_dec8_taylor_break(ln_x, &delta, (int32)prec)) {
            break;
        }
    }
}

static inline status_t cm_dec8_ln_op(const dec8_t *dec, dec8_t *result, uint32 prec)
{
    if (DEC_IS_NEGATIVE(dec) || DECIMAL_IS_ZERO(dec)) {
        GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "argument must be greater than 0");
        return GS_ERROR;
    }

    DEC8_DEBUG_PRINT(dec, "go into ln(x) with expn = %d", dec->expn);
    if (dec->expn == 0) {
        cm_dec8_ln_small(dec, result, prec);
        return GS_SUCCESS;
    } else {
        dec8_t x;
        int32 exponent = D8EXP_2_SEXP(dec->expn);
        cm_dec8_copy(&x, dec);
        x.expn = 0;
        // set y to ln(x)
        cm_dec8_ln_small(&x, result, prec); // ln(x)

        // set x to exponent  * ln(10)
        cm_int32_to_dec8(exponent, &x);
        (void)cm_dec8_mul_op(&x, &DEC8_LN10, &x);

        // set result to x + y
        return cm_dec8_add_op(result, &x, result);
    }
}

status_t cm_dec8_ln(const dec8_t *dec, dec8_t *result)
{
    DEC8_DEBUG_PRINT(dec, "go into ln(dec)");
    if (cm_dec8_ln_op(dec, result, MAX_NUM_CMP_PREC) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return cm_dec8_finalise(result, MAX_NUMERIC_BUFF, GS_FALSE);
}

// 2. log(n2, n1) = ln(n1) / ln(n2), where n2 > 0 && n2 != 1 && n1 > 0,
status_t cm_dec8_log(const dec8_t *n2, const dec8_t *n1, dec8_t *result)
{
    dec8_t ln_n1;
    dec8_t ln_n2;

    if (cm_dec8_is_one(n2)) {
        GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "the first argument can not be 0 or 1");
        return GS_ERROR;
    }

    if (cm_dec8_ln_op(n2, &ln_n2, MAX_NUM_CMP_PREC) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (cm_dec8_ln_op(n1, &ln_n1, MAX_NUM_CMP_PREC) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return cm_dec8_divide(&ln_n1, &ln_n2, result);
}

/* Compute the y = x^r
 * + if x < 0 and r is non-integer return error
 * + if x = 0 && r < 0 return error
 * @author Added by 2018/07/31
*  */
status_t cm_dec8_power(const dec8_t *x, const dec8_t *r, dec8_t *y)
{
    if (DECIMAL_IS_ZERO(x)) {
        if (DEC_IS_NEGATIVE(r)) {
            GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "invalid argument for POWER function");
            return GS_ERROR;
        }
        if (DECIMAL_IS_ZERO(r)) { // whether 0^0 is equal to 1
            cm_dec8_copy(y, &DEC8_ONE);
            return GS_SUCCESS;
        }
        cm_zero_dec8(y);
        return GS_SUCCESS;
    }

    dec8_t abs_a;
    bool32 is_neg = GS_FALSE;
    const dec8_t *pa = x;

    if (DEC_IS_NEGATIVE(x)) {
        if (!cm_dec8_is_integer(r)) {
            GS_THROW_ERROR(ERR_INVALID_FUNC_PARAMS, "invalid argument for POWER function");
            return GS_ERROR;
        }
        if (cm_dec8_is_odd(r)) {
            is_neg = GS_TRUE;
        }
        cm_dec8_copy(&abs_a, x);
        pa = cm_dec8_abs(&abs_a);
    }

    // extra 5 precisions for achieve a higher computation precision
    GS_RETURN_IFERR(cm_dec8_ln_op(pa, y, MAX_NUM_CMP_PREC + 5)); // set y to ln(abs(x))
    (void)cm_dec8_mul_op(y, r, &abs_a);
    GS_RETURN_IFERR(cm_dec8_exp_op(&abs_a, y, MAX_NUM_CMP_PREC));

    // here y >= 0
    if (is_neg && !DECIMAL_IS_ZERO(y)) {
        y->sign = DEC_SIGN_MINUS;
    }

    return cm_dec8_finalise(y, MAX_NUMERIC_BUFF, GS_FALSE);
}

/* * set y to n2 - n1 * FLOOR(n2 / n1)
 * * y must have the same sign of n2 */
status_t cm_dec8_mod(const dec8_t *n2, const dec8_t *n1, dec8_t *y)
{
    if (DECIMAL_IS_ZERO(n1)) {
        cm_dec8_copy(y, n2);
        return GS_SUCCESS;
    }

    dec8_t z;

    // set y to n2 / n1
    cm_dec8_inv(n1, &z);
    (void)cm_dec8_mul_op(n2, &z, y);
    (void)cm_dec8_finalise(y, GS_MAX_DEC_OUTPUT_PREC, GS_TRUE);

    // set z to floor(y) *n1
    (void)cm_dec8_floor(y);
    (void)cm_dec8_mul_op(y, n1, &z);
    (void)cm_dec8_finalise(&z, GS_MAX_DEC_OUTPUT_PREC, GS_TRUE);

    // set y to n2 - z
    (void)cm_dec8_sub_op(n2, &z, y);
    (void)cm_dec8_finalise(y, GS_MAX_DEC_OUTPUT_PREC, GS_TRUE);

    if (DECIMAL_IS_ZERO(y)) {
        return GS_SUCCESS;
    }

    // for illegal cases return 0
    bool32 cond = (n1->sign == n2->sign && n2->sign != y->sign);
    if (cond) {
        cm_zero_dec8(y);
        return GS_SUCCESS;
    }

    /* to ensure y has the same SIGN of n2 */
    cond = (n2->sign != n1->sign && n2->sign != y->sign);
    if (cond) {
        (void)cm_dec8_sub_op(y, n1, y);
    }

    return cm_dec8_finalise(y, MAX_NUMERIC_BUFF, GS_FALSE);
}

/**
 * Compute the sign of a decimal
 * @author Added 2018/08/21
 */
void cm_dec8_sign(const dec8_t *dec, dec8_t *result)
{
    if (DECIMAL_IS_ZERO(dec)) {
        cm_dec8_copy(result, &DEC8_ZERO);
        return;
    }

    if (DEC_IS_NEGATIVE(dec)) {
        cm_dec8_copy(result, &DEC8_NEG_ONE);
    } else {
        cm_dec8_copy(result, &DEC8_ONE);
    }
}

/**
 * Use for debugging. see the macro @DEC_DEBUG_PRINT
 */
void cm_dec8_print(const dec8_t *dec, const char *file, uint32 line, const char *func_name, const char *fmt, ...)
{
    char buf[100];
    va_list var_list;
    dec8_t fl_dec;

    printf("%s:%u:%s\n", file, line, func_name);
    va_start(var_list, fmt);
    PRTS_RETVOID_IFERR(vsnprintf_s(buf, sizeof(buf), sizeof(buf) - 1, fmt, var_list));

    va_end(var_list);
    printf("%s\n", buf);
    (void)cm_dec8_to_str_all(dec, buf, sizeof(buf));
    printf("dec := %s\n", buf);
    printf("  ncells = %u, expn = %d, sign = %c, bytes = %u\n", dec->ncells, dec->expn,
        (DEC_IS_NEGATIVE(dec)) ? '-' : '+', (uint32)cm_dec8_stor_sz(dec));
    printf("  cells = { ");
    for (uint32 i = 0; i < (uint32)dec->ncells; i++) {
        if (i != 0) {
            printf(", ");
        }
        printf("%08u", dec->cells[i]);
    }
    printf("}\n");

    fl_dec = *dec;
    (void)cm_dec8_finalise(&fl_dec, MAX_NUM_CMP_PREC, GS_TRUE);
    (void)cm_dec8_to_str_all(&fl_dec, buf, sizeof(buf));
    printf("finalized dec := %s\n\n", buf);
    (void)fflush(stdout);
}

#ifdef __cplusplus
}
#endif
