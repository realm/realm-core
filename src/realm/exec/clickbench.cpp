#include <ctime>
#include <iostream>
#include <fstream>
#include <cstring>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <realm.hpp>
#include <realm/cluster.hpp>
#include <realm/db.hpp>
#include <realm/history.hpp>

using namespace realm;

template <class T>
class Mailbox {
public:
    void send(T* val)
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        m_list.push_back(val);
        m_cv.notify_one();
    }
    T* receive()
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        while (m_list.empty())
            m_cv.wait(lck);
        T* ret = m_list.front();
        m_list.pop_front();
        return ret;
    }

private:
    std::deque<T*> m_list;
    std::mutex m_mutex;
    std::condition_variable m_cv;
};

// remove this when enumerated strings are supported:
#define type_EnumString type_String

static void create_table(TransactionRef tr)
{
    auto t = tr->add_table("Hits");
    t->add_column(type_Int, "WatchID");
    t->add_column(type_Int, "JavaEnable");
    t->add_column(type_EnumString, "Title", true);
    t->add_column(type_Int, "GoodEvent");
    t->add_column(type_Timestamp, "EventTime");
    t->add_column(type_Timestamp, "EventDate");
    t->add_column(type_Int, "CounterID");
    t->add_column(type_Int, "ClientIP");
    t->add_column(type_Int, "RegionID");
    t->add_column(type_Int, "UserID");
    t->add_column(type_Int, "CounterClass");
    t->add_column(type_Int, "OS");
    t->add_column(type_Int, "UserAgent");
    t->add_column(type_EnumString, "URL", true);
    t->add_column(type_EnumString, "Referer", true);
    t->add_column(type_Int, "IsRefresh");
    t->add_column(type_Int, "RefererCategoryID");
    t->add_column(type_Int, "RefererRegionID");
    t->add_column(type_Int, "URLCategoryID");
    t->add_column(type_Int, "URLRegionID");
    t->add_column(type_Int, "ResolutionWidth");
    t->add_column(type_Int, "ResolutionHeight");
    t->add_column(type_Int, "ResolutionDepth");
    t->add_column(type_Int, "FlashMajor");
    t->add_column(type_Int, "FlashMinor");
    t->add_column(type_EnumString, "FlashMinor2", true);
    t->add_column(type_Int, "NetMajor");
    t->add_column(type_Int, "NetMinor");
    t->add_column(type_Int, "UserAgentMajor");
    t->add_column(type_EnumString, "UserAgentMinor", true);
    t->add_column(type_Int, "CookieEnable");
    t->add_column(type_Int, "JavascriptEnable");
    t->add_column(type_Int, "IsMobile");
    t->add_column(type_Int, "MobilePhone");
    t->add_column(type_EnumString, "MobilePhoneModel", true);
    t->add_column(type_EnumString, "Params", true);
    t->add_column(type_Int, "IPNetworkID");
    t->add_column(type_Int, "TraficSourceID");
    t->add_column(type_Int, "SearchEngineID");
    t->add_column(type_EnumString, "SearchPhrase", true);
    t->add_column(type_Int, "AdvEngineID");
    t->add_column(type_Int, "IsArtifical");
    t->add_column(type_Int, "WindowClientWidth");
    t->add_column(type_Int, "WindowClientHeight");
    t->add_column(type_Int, "ClientTimeZone");
    t->add_column(type_Timestamp, "ClientEventTime");
    t->add_column(type_Int, "SilverlightVersion1");
    t->add_column(type_Int, "SilverlightVersion2");
    t->add_column(type_Int, "SilverlightVersion3");
    t->add_column(type_Int, "SilverlightVersion4");
    t->add_column(type_EnumString, "PageCharset", true);
    t->add_column(type_Int, "CodeVersion");
    t->add_column(type_Int, "IsLink");
    t->add_column(type_Int, "IsDownload");
    t->add_column(type_Int, "IsNotBounce");
    t->add_column(type_Int, "FUniqID");
    t->add_column(type_EnumString, "OriginalURL", true);
    t->add_column(type_Int, "HID");
    t->add_column(type_Int, "IsOldCounter");
    t->add_column(type_Int, "IsEvent");
    t->add_column(type_Int, "IsParameter");
    t->add_column(type_Int, "DontCountHits");
    t->add_column(type_Int, "WithHash");
    t->add_column(type_EnumString, "HitColor", true);
    t->add_column(type_Timestamp, "LocalEventTime");
    t->add_column(type_Int, "Age");
    t->add_column(type_Int, "Sex");
    t->add_column(type_Int, "Income");
    t->add_column(type_Int, "Interests");
    t->add_column(type_Int, "Robotness");
    t->add_column(type_Int, "RemoteIP");
    t->add_column(type_Int, "WindowName");
    t->add_column(type_Int, "OpenerName");
    t->add_column(type_Int, "HistoryLength");
    t->add_column(type_EnumString, "BrowserLanguage", true);
    t->add_column(type_EnumString, "BrowserCountry", true);
    t->add_column(type_EnumString, "SocialNetwork", true);
    t->add_column(type_EnumString, "SocialAction", true);
    t->add_column(type_Int, "HTTPError");
    t->add_column(type_Int, "SendTiming");
    t->add_column(type_Int, "DNSTiming");
    t->add_column(type_Int, "ConnectTiming");
    t->add_column(type_Int, "ResponseStartTiming");
    t->add_column(type_Int, "ResponseEndTiming");
    t->add_column(type_Int, "FetchTiming");
    t->add_column(type_Int, "SocialSourceNetworkID");
    t->add_column(type_EnumString, "SocialSourcePage", true);
    t->add_column(type_Int, "ParamPrice");
    t->add_column(type_EnumString, "ParamOrderID", true);
    t->add_column(type_EnumString, "ParamCurrency", true);
    t->add_column(type_Int, "ParamCurrencyID");
    t->add_column(type_EnumString, "OpenstatServiceName", true);
    t->add_column(type_EnumString, "OpenstatCampaignID", true);
    t->add_column(type_EnumString, "OpenstatAdID", true);
    t->add_column(type_EnumString, "OpenstatSourceID", true);
    t->add_column(type_EnumString, "UTMSource", true);
    t->add_column(type_EnumString, "UTMMedium", true);
    t->add_column(type_EnumString, "UTMCampaign", true);
    t->add_column(type_EnumString, "UTMContent", true);
    t->add_column(type_EnumString, "UTMTerm", true);
    t->add_column(type_EnumString, "FromTag", true);
    t->add_column(type_Int, "HasGCLID");
    t->add_column(type_Int, "RefererHash");
    t->add_column(type_Int, "URLHash");
    t->add_column(type_Int, "CLID");
    tr->commit();
}

static int strtoi(const char* p, char** endp)
{
    int ret = 0;
    while (*p >= '0' && *p <= '9') {
        ret *= 10;
        ret += *p - '0';
        ++p;
    }
    *endp = const_cast<char*>(p);
    return ret;
}

inline int64_t epoch_days_fast(int y, int m, int d)
{
    const uint32_t year_base = 4800; /* Before min year, multiple of 400. */
    const int32_t m_adj = m - 3;     /* March-based month. */
    const uint32_t carry = (m_adj > m) ? 1 : 0;
    const uint32_t adjust = carry ? 12 : 0;
    const uint32_t y_adj = y + year_base - carry;
    const uint32_t month_days = ((m_adj + adjust) * 62719 + 769) / 2048;
    const uint32_t leap_days = y_adj / 4 - y_adj / 100 + y_adj / 400;
    return y_adj * 365 + leap_days + month_days + (d - 1) - 2472632;
}

static Timestamp get_timestamp(const char* str)
{
    char* p;
    int year = int(strtoi(str, &p));
    if (*p == '-') {
        p++;
        int mon = int(strtoi(p, &p));
        if (*p == '-') {
            p++;
            int day = int(strtoi(p, &p));
            time_t hms = 0;
            if (*p == ' ' || *p == 'T') {
                p++;
                int h = int(strtoi(p, &p));
                int m = 0;
                int s = 0;
                if (*p == ':') {
                    p++;
                    m = int(strtoi(p, &p));
                    if (*p == ':') {
                        p++;
                        s = int(strtoi(p, &p));
                    }
                }
                hms = (h * 3600) + (m * 60) + s;
            }
            if (*p == '\0') {
                return Timestamp(epoch_days_fast(year, mon, day) * 86400 + hms, 0);
            }
        }
    }
    return Timestamp();
}

struct BufferedValues {
    std::vector<std::string> buffer{256};
    std::vector<FieldValues> values{256};
};

Mailbox<BufferedValues> mbx;
Mailbox<BufferedValues> resp;

void parse_file(const char* filename)
{
    std::ifstream inp(filename);

    auto buf = resp.receive();
    auto str = buf->buffer.begin();
    auto it = buf->values.begin();
    auto end = buf->values.end();
    while (std::getline(inp, *str)) {
        char* tok = str->data();
        for (FieldValue& val : *it) {
            char* end = strchr(tok, '\t');
            if (end) {
                *end = '\0';
            }
            switch (val.col_key.get_type()) {
                case col_type_Int:
                    val.value = Mixed(int64_t(strtoll(tok, nullptr, 10)));
                    break;
                case col_type_String:
                    val.value = strlen(tok) ? Mixed(tok) : Mixed();
                    break;
                    /*
                                    case col_type_EnumString:
                                        val.value = strlen(tok) ? Mixed(tok) : Mixed();
                                        break;
                    */
                case col_type_Timestamp:
                    val.value = Mixed(get_timestamp(tok));
                    break;
                default:
                    break;
            }
            tok = end + 1;
        }
        ++it;
        ++str;
        if (it == end) {
            mbx.send(buf);
            buf = resp.receive();
            str = buf->buffer.begin();
            it = buf->values.begin();
            end = buf->values.end();
        }
    }
    buf->values.erase(it, end);
    if (buf->values.size()) {
        mbx.send(buf);
    }
    mbx.send(nullptr);
}

void import(const char* filename)
{
    DBOptions options;
    auto db = DB::create(make_in_realm_history(), "hits.realm");
    create_table(db->start_write());
    auto tr = db->start_write();
    auto t = tr->get_table("Hits");
    auto col_keys = t->get_column_keys();

    std::cout << std::endl << "Reading data into realm" << std::endl;
    auto time_start = std::chrono::high_resolution_clock::now();
    BufferedValues buf1;
    BufferedValues buf2;
    for (auto& val : buf1.values) {
        for (auto col : col_keys) {
            val.insert(col, Mixed());
        }
    }
    for (auto& val : buf2.values) {
        for (auto col : col_keys) {
            val.insert(col, Mixed());
        }
    }
    resp.send(&buf1);
    resp.send(&buf2);
    std::thread parse_file_thread(parse_file, filename);

    int buf_cnt = 0;
    const int bufs_per_commit = 100;
    while (auto buf = mbx.receive()) {
        // t->create_objects(buf);
        for (auto& val : buf->values) {
            Obj o = t->create_object(ObjKey(), val);
            // verify
            /*
                        for (auto& e : val) {
                            if (e.col_key.get_type() == col_type_String) {
                                auto got_string = o.get<StringData>(e.col_key);
                                auto the_string = e.value.get_string();
                                REALM_ASSERT(got_string == the_string);
                            }
                        }
            */
        }
        resp.send(buf);
        if (buf_cnt++ > bufs_per_commit) {
            tr->commit_and_continue_as_read();
            tr->promote_to_write();
            std::cout << '.';
            std::cout.flush();
            buf_cnt = 0;
        }
    }
    tr->commit_and_continue_as_read();

    parse_file_thread.join();
    auto time_end = std::chrono::high_resolution_clock::now();
    std::cout << "Ingestion complete in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << " msecs"
              << std::endl;
    /*
        std::cout << std::endl;
        t->dump_interning_stats();
        std::cout << std::endl;
        std::cout << t->size() << std::endl;
    */
}

void dump_prop(const char* filename, const char* prop_name)
{
    auto db = DB::create(make_in_realm_history(), filename);
    auto tr = db->start_read();
    auto t = tr->get_table("Hits");
    auto col = t->get_column_key(prop_name);
    for (auto& o : *t) {
        switch (col.get_type()) {
            case col_type_Int:
                std::cout << o.get<Int>(col) << std::endl;
                break;
            case col_type_String:
                std::cout << o.get<String>(col) << std::endl;
                break;
                /*
                            case col_type_EnumString:
                                REALM_ASSERT(false);
                                break;
                */
            case col_type_Timestamp:
                std::cout << o.get<Timestamp>(col) << std::endl;
                break;
            default:
                break;
        }
    }
}

int main(int argc, const char* argv[])
{
    if (argc == 1) {
        import("/home/finn/Downloads/mill.tsv");
    }
    if (argc == 2) {
        import(argv[1]);
    }
    if (argc == 3) {
        dump_prop(argv[1], argv[2]);
    }
}
