#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>
#include <string.h>

// araç sayıları
#define ARABA_SAYISI 12
#define MINIBUS_SAYISI 10
#define KAMYON_SAYISI 8
#define TOPLAM_ARAC (ARABA_SAYISI + MINIBUS_SAYISI + KAMYON_SAYISI)

// feribot max kapasite
#define KAPASITE 20
// her yakada 2 gişe var
#define GISE_SAYISI 2

// ms cinsinden gecikmeler
#define MAX_BEKLEME_MS 3000
#define GISE_GECIKME 200
#define BINIS_GECIKME 150
#define YOLCULUK_MS 800
#define DONUS_BEKLEME 500

// araç tipleri
typedef enum
{
    ARABA = 0,
    MINIBUS,
    KAMYON
} AracTipi;

// her tipin kaç birim kapladığı
static const char *tip_adi[3] = {"Araba", "Minibus", "Kamyon"};

// simülasyon başlangıç zamanı
static struct timespec baslangic;

// kaç ms geçti
static long kac_ms_gecti()
{
    struct timespec simdi;
    clock_gettime(CLOCK_MONOTONIC, &simdi);
    long ms = (simdi.tv_sec - baslangic.tv_sec) * 1000L + (simdi.tv_nsec - baslangic.tv_nsec) / 1000000L;
    return ms;
}

// log için mutex
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

// log fonksiyonu - yarış koşulu olmasın diye mutex kullandım
#define LOG(fmt, ...)                                                    \
    do                                                                   \
    {                                                                    \
        pthread_mutex_lock(&log_mutex);                                  \
        printf("[Zaman %4ld] " fmt "\n", kac_ms_gecti(), ##__VA_ARGS__); \
        fflush(stdout);                                                  \
        pthread_mutex_unlock(&log_mutex);                                \
    } while (0)

// istatistik değişkenleri
static pthread_mutex_t istat_mutex = PTHREAD_MUTEX_INITIALIZER;
static long toplam_bekleme = 0;
static long maks_bekleme = 0;
static int sefer_sayisi = 0;
static long toplam_doluluk = 0;

static void beklemeyi_kaydet(long ms)
{
    pthread_mutex_lock(&istat_mutex);
    toplam_bekleme += ms;
    if (ms > maks_bekleme)
        maks_bekleme = ms;
    pthread_mutex_unlock(&istat_mutex);
}

// FIFO kuyruk yapısı
typedef struct DugumYapisi
{
    int arac_id;
    int birim_sayisi;
    struct DugumYapisi *sonraki;
} Dugum;

typedef struct
{
    Dugum *bas, *son;
    int eleman_sayisi;
    int toplam_birim;
    pthread_mutex_t mutex;
} Kuyruk;

static void kuyruk_baslat(Kuyruk *k)
{
    k->bas = k->son = NULL;
    k->eleman_sayisi = 0;
    k->toplam_birim = 0;
    pthread_mutex_init(&k->mutex, NULL);
}

// kuyruğa araç ekle
static void kuyruga_ekle(Kuyruk *k, int id, int b)
{
    Dugum *yeni = malloc(sizeof(Dugum));
    yeni->arac_id = id;
    yeni->birim_sayisi = b;
    yeni->sonraki = NULL;
    pthread_mutex_lock(&k->mutex);
    if (k->son)
        k->son->sonraki = yeni;
    else
        k->bas = yeni;
    k->son = yeni;
    k->eleman_sayisi++;
    k->toplam_birim += b;
    pthread_mutex_unlock(&k->mutex);
}

// kapasiteye sığan ilk aracı çıkar
static int kuyruktan_al(Kuyruk *k, int kalan, int *cikan_birim)
{
    pthread_mutex_lock(&k->mutex);
    Dugum *onceki = NULL, *simdiki = k->bas;
    while (simdiki)
    {
        if (simdiki->birim_sayisi <= kalan)
        {
            int id = simdiki->arac_id;
            *cikan_birim = simdiki->birim_sayisi;
            if (onceki)
                onceki->sonraki = simdiki->sonraki;
            else
                k->bas = simdiki->sonraki;
            if (!simdiki->sonraki)
                k->son = onceki;
            k->eleman_sayisi--;
            k->toplam_birim -= simdiki->birim_sayisi;
            free(simdiki);
            pthread_mutex_unlock(&k->mutex);
            return id;
        }
        onceki = simdiki;
        simdiki = simdiki->sonraki;
    }
    pthread_mutex_unlock(&k->mutex);
    return -1; // sığan araç yok
}

// bekleme kuyrukları (0=A yakası, 1=B yakası)
static Kuyruk bekleme[2];
// gişe semaforları
static sem_t gise[2];

// feribot durumu
static pthread_mutex_t feribot_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t feribot_cond = PTHREAD_COND_INITIALIZER;
static int feribot_yaka = -1;
static int feribot_yuk = 0;
static int sim_bitti = 0;

// her araç için senkronizasyon
typedef struct
{
    pthread_mutex_t mx;
    pthread_cond_t cv;
    int bindi;
    int karsi_yakaya_gecti;
    int indi;
} AracSync;

static AracSync async[TOPLAM_ARAC];

// araç bilgisi
typedef struct
{
    int id;
    AracTipi tip;
    int birim;
    int baslangic_yaka;
} Arac;

static Arac araclar[TOPLAM_ARAC];

// biten araç sayısı
static pthread_mutex_t bitis_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t bitis_cond = PTHREAD_COND_INITIALIZER;
static int tamamlanan = 0;

// ms uyku
static void ms_bekle(int ms)
{
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

static int rastgele(int alt, int ust)
{
    return alt + rand() % (ust - alt + 1);
}

// araç thread fonksiyonu
static void *arac_thread(void *arg)
{
    Arac *a = arg;
    int simdiki_yaka = a->baslangic_yaka;

    LOG("%s-%d %c yakasinda olusturuldu", tip_adi[a->tip], a->id, 'A' + simdiki_yaka);

    // gidis + donus = 2 tur
    for (int tur = 0; tur < 2; tur++)
    {

        // gişeye git
        LOG("%s-%d %c yakasinda giseye yaklasti", tip_adi[a->tip], a->id, 'A' + simdiki_yaka);
        sem_wait(&gise[simdiki_yaka]);
        LOG("%s-%d giseden gecti", tip_adi[a->tip], a->id);
        ms_bekle(rastgele(GISE_GECIKME / 2, GISE_GECIKME));
        sem_post(&gise[simdiki_yaka]);

        // kuyruğa gir
        long bekleme_baslangic = kac_ms_gecti();
        kuyruga_ekle(&bekleme[simdiki_yaka], a->id, a->birim);
        LOG("%s-%d %c yakasi kuyruğuna girdi (birim=%d)",
            tip_adi[a->tip], a->id, 'A' + simdiki_yaka, a->birim);

        // feribot bindirinceye kadar bekle
        pthread_mutex_lock(&async[a->id].mx);
        async[a->id].bindi = 0;
        async[a->id].karsi_yakaya_gecti = 0;
        async[a->id].indi = 0;
        while (!async[a->id].bindi)
            pthread_cond_wait(&async[a->id].cv, &async[a->id].mx);
        pthread_mutex_unlock(&async[a->id].mx);

        long bekledi = kac_ms_gecti() - bekleme_baslangic;
        beklemeyi_kaydet(bekledi);
        LOG("%s-%d feribota bindi (%ld ms bekledi)", tip_adi[a->tip], a->id, bekledi);

        // karşı yakaya geçmeyi bekle
        pthread_mutex_lock(&async[a->id].mx);
        while (!async[a->id].karsi_yakaya_gecti)
            pthread_cond_wait(&async[a->id].cv, &async[a->id].mx);
        pthread_mutex_unlock(&async[a->id].mx);

        LOG("%s-%d %c yakasina ulasti", tip_adi[a->tip], a->id, 'A' + (1 - simdiki_yaka));

        // indim sinyali ver
        pthread_mutex_lock(&async[a->id].mx);
        async[a->id].indi = 1;
        pthread_cond_signal(&async[a->id].cv);
        pthread_mutex_unlock(&async[a->id].mx);

        simdiki_yaka = 1 - simdiki_yaka;

        // dönüş turu için bekle
        if (tur == 0)
        {
            int gecikme = rastgele(DONUS_BEKLEME / 2, DONUS_BEKLEME * 2);
            LOG("%s-%d donus icin %d ms bekliyor", tip_adi[a->tip], a->id, gecikme);
            ms_bekle(gecikme);
        }
    }

    LOG("%s-%d tam turu tamamladi", tip_adi[a->tip], a->id);

    pthread_mutex_lock(&bitis_mutex);
    tamamlanan++;
    if (tamamlanan == TOPLAM_ARAC)
    {
        sim_bitti = 1;
        pthread_cond_signal(&bitis_cond);
        pthread_mutex_lock(&feribot_mutex);
        pthread_cond_signal(&feribot_cond);
        pthread_mutex_unlock(&feribot_mutex);
    }
    pthread_mutex_unlock(&bitis_mutex);

    return NULL;
}

// feribot thread fonksiyonu
static void *feribot_thread(void *arg)
{
    (void)arg;

    feribot_yaka = rand() % 2;
    LOG("Feribot %c yakasinda basladi", 'A' + feribot_yaka);

    while (1)
    {
        pthread_mutex_lock(&feribot_mutex);
        if (sim_bitti)
        {
            pthread_mutex_unlock(&feribot_mutex);
            break;
        }
        pthread_mutex_unlock(&feribot_mutex);

        // yükleme başla
        feribot_yuk = 0;
        LOG("Feribot %c yakasinda yukleme basliyor", 'A' + feribot_yaka);

        long kalkis_zamani = kac_ms_gecti() + MAX_BEKLEME_MS;
        int binen_idler[TOPLAM_ARAC];
        int binen_sayisi = 0;

        while (1)
        {
            int kalan = KAPASITE - feribot_yuk;

            // sığacak araç var mı bak
            pthread_mutex_lock(&bekleme[feribot_yaka].mutex);
            int sigacak_var = 0;
            Dugum *d = bekleme[feribot_yaka].bas;
            while (d)
            {
                if (d->birim_sayisi <= kalan)
                {
                    sigacak_var = 1;
                    break;
                }
                d = d->sonraki;
            }
            pthread_mutex_unlock(&bekleme[feribot_yaka].mutex);

            if (!sigacak_var)
                break;
            if (kac_ms_gecti() >= kalkis_zamani)
                break; // zaman doldu

            int fit_birim;
            int vid = kuyruktan_al(&bekleme[feribot_yaka], kalan, &fit_birim);
            if (vid < 0)
                break;

            ms_bekle(rastgele(BINIS_GECIKME / 2, BINIS_GECIKME));
            feribot_yuk += fit_birim;

            LOG("Feribot %s-%d'yi yukLedi (toplam yuk=%d/%d)",
                tip_adi[araclar[vid].tip], vid, feribot_yuk, KAPASITE);

            // araca "bindin" sinyali gönder
            pthread_mutex_lock(&async[vid].mx);
            async[vid].bindi = 1;
            pthread_cond_signal(&async[vid].cv);
            pthread_mutex_unlock(&async[vid].mx);

            binen_idler[binen_sayisi++] = vid;
        }

        if (binen_sayisi == 0)
        {
            // bu yakada kimse yoksa karşıya bak
            int karsi_var = bekleme[1 - feribot_yaka].eleman_sayisi;
            if (!karsi_var)
            {
                pthread_mutex_lock(&bitis_mutex);
                int hepsi_bitti = (tamamlanan == TOPLAM_ARAC);
                pthread_mutex_unlock(&bitis_mutex);
                if (hepsi_bitti)
                    break;
                ms_bekle(100);
                continue;
            }
        }

        // istatistik güncelle
        pthread_mutex_lock(&istat_mutex);
        sefer_sayisi++;
        toplam_doluluk += feribot_yuk;
        pthread_mutex_unlock(&istat_mutex);

        LOG("Feribot %c yakasından kalkti (yuk=%d, arac=%d)",
            'A' + feribot_yaka, feribot_yuk, binen_sayisi);

        ms_bekle(YOLCULUK_MS);
        feribot_yaka = 1 - feribot_yaka;
        LOG("Feribot %c yakasina ulasti", 'A' + feribot_yaka);

        // indirme
        LOG("Feribot %c yakasinda bosaltma basliyor", 'A' + feribot_yaka);
        for (int i = 0; i < binen_sayisi; i++)
        {
            int vid = binen_idler[i];

            pthread_mutex_lock(&async[vid].mx);
            async[vid].karsi_yakaya_gecti = 1;
            pthread_cond_signal(&async[vid].cv);
            pthread_mutex_unlock(&async[vid].mx);

            // araç inene kadar bekle
            pthread_mutex_lock(&async[vid].mx);
            while (!async[vid].indi)
                pthread_cond_wait(&async[vid].cv, &async[vid].mx);
            pthread_mutex_unlock(&async[vid].mx);

            LOG("%s-%d %c yakasinda indi",
                tip_adi[araclar[vid].tip], vid, 'A' + feribot_yaka);
        }
        LOG("Feribot bosaltma tamamlandi");
    }

    LOG("Feribot thread bitti");
    return NULL;
}

int main()
{
    srand((unsigned)time(NULL));
    clock_gettime(CLOCK_MONOTONIC, &baslangic);

    // araçları tanımla ve rastgele yaka ata
    int idx = 0;
    for (int i = 0; i < ARABA_SAYISI; i++, idx++)
        araclar[idx] = (Arac){idx, ARABA, 1, rand() % 2};
    for (int i = 0; i < MINIBUS_SAYISI; i++, idx++)
        araclar[idx] = (Arac){idx, MINIBUS, 2, rand() % 2};
    for (int i = 0; i < KAMYON_SAYISI; i++, idx++)
        araclar[idx] = (Arac){idx, KAMYON, 3, rand() % 2};

    kuyruk_baslat(&bekleme[0]);
    kuyruk_baslat(&bekleme[1]);
    sem_init(&gise[0], 0, GISE_SAYISI);
    sem_init(&gise[1], 0, GISE_SAYISI);

    for (int i = 0; i < TOPLAM_ARAC; i++)
    {
        pthread_mutex_init(&async[i].mx, NULL);
        pthread_cond_init(&async[i].cv, NULL);
        async[i].bindi = async[i].karsi_yakaya_gecti = async[i].indi = 0;
    }

    printf("=== Feribot Tasimaciligi Simulasyonu ===\n");
    printf("Araclar: %d Araba, %d Minibus, %d Kamyon | Kapasite: %d birim\n\n",
           ARABA_SAYISI, MINIBUS_SAYISI, KAMYON_SAYISI, KAPASITE);

    pthread_t feribot_tid;
    pthread_t arac_tidler[TOPLAM_ARAC];

    pthread_create(&feribot_tid, NULL, feribot_thread, NULL);
    for (int i = 0; i < TOPLAM_ARAC; i++)
        pthread_create(&arac_tidler[i], NULL, arac_thread, &araclar[i]);

    // hepsi bitene kadar bekle
    pthread_mutex_lock(&bitis_mutex);
    while (tamamlanan < TOPLAM_ARAC)
        pthread_cond_wait(&bitis_cond, &bitis_mutex);
    pthread_mutex_unlock(&bitis_mutex);

    pthread_join(feribot_tid, NULL);
    for (int i = 0; i < TOPLAM_ARAC; i++)
        pthread_join(arac_tidler[i], NULL);

    long sim_suresi = kac_ms_gecti();

    printf("\n=== SIMULASYON ISTATISTIKLERI ===\n");
    printf("Toplam sure         : %ld ms\n", sim_suresi);
    printf("Toplam arac         : %d\n", TOPLAM_ARAC);
    printf("Feribot sefer sayisi: %d\n", sefer_sayisi);
    printf("Ort. bekleme suresi : %.1f ms\n",
           TOPLAM_ARAC ? (double)toplam_bekleme / (TOPLAM_ARAC * 2) : 0.0);
    printf("Maks bekleme suresi : %ld ms\n", maks_bekleme);
    double doluluk = sefer_sayisi
                         ? (double)toplam_doluluk / (sefer_sayisi * KAPASITE) * 100.0
                         : 0.0;
    printf("Feribot doluluk     : %.1f%%\n", doluluk);

    sem_destroy(&gise[0]);
    sem_destroy(&gise[1]);
    for (int i = 0; i < TOPLAM_ARAC; i++)
    {
        pthread_mutex_destroy(&async[i].mx);
        pthread_cond_destroy(&async[i].cv);
    }

    return 0;
}
