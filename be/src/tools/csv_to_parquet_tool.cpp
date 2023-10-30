#include "common/logging.h"
#include "common/object_pool.h"
#include "io/fs/file_writer.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/csv/csv_reader.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/runtime/vparquet_writer.h"

using doris::RuntimeProfile;
using doris::SlotDescriptor;
using doris::TParquetDataType;
using doris::TSlotDescriptorBuilder;
using doris::vectorized::CsvReader;

DEFINE_string(input_file, "", "input csv file path");
DEFINE_string(output_file, "", "output parquet file path");
DEFINE_string(columns_type, "", "columns type, such as INT,BOOLEAN");
DEFINE_string(format, "csv", "input file type, now only support csv");
DEFINE_string(column_separator, "|", "column separator for csv format");
DEFINE_string(line_delimiter, "\n", "line delimiter for csv format");
DEFINE_int64(max_output_lines, std::numeric_limits<int64_t>::max(), "max output lines for each output file");

std::map<std::string, std::string> tpcds_tbl_columns = {
{"dbgen_version", "dv_version varchar(16),dv_create_date date,dv_create_time datetime,dv_cmdline_args varchar(200)"},
{"call_center", "cc_call_center_sk integer not null,cc_call_center_id char(16) not null,cc_rec_start_date date,cc_rec_end_date date,cc_closed_date_sk integer,cc_open_date_sk integer,cc_name varchar(50),cc_class varchar(50),cc_employees integer,cc_sq_ft integer,cc_hours char(20),cc_manager varchar(40),cc_mkt_id integer,cc_mkt_class char(50),cc_mkt_desc varchar(100),cc_market_manager varchar(40),cc_division integer,cc_division_name varchar(50),cc_company integer,cc_company_name char(50),cc_street_number char(10),cc_street_name varchar(60),cc_street_type char(15),cc_suite_number char(10),cc_city varchar(60),cc_county varchar(30),cc_state char(2),cc_zip char(10),cc_country varchar(20),cc_gmt_offset decimal(5,2),cc_tax_percentage decimal(5,2)"},
{"catalog_page", "cp_catalog_page_sk integer not null,cp_catalog_page_id char(16) not null,cp_start_date_sk integer,cp_end_date_sk integer,cp_department varchar(50),cp_catalog_number integer,cp_catalog_page_number integer,cp_description varchar(100),cp_type varchar(100)"},
{"catalog_returns", "cr_returned_date_sk integer,cr_returned_time_sk integer,cr_item_sk integer not null,cr_refunded_customer_sk integer,cr_refunded_cdemo_sk integer,cr_refunded_hdemo_sk integer,cr_refunded_addr_sk integer,cr_returning_customer_sk integer,cr_returning_cdemo_sk integer,cr_returning_hdemo_sk integer,cr_returning_addr_sk integer,cr_call_center_sk integer,cr_catalog_page_sk integer,cr_ship_mode_sk integer,cr_warehouse_sk integer,cr_reason_sk integer,cr_order_number integer not null,cr_return_quantity integer,cr_return_amount decimal(7,2),cr_return_tax decimal(7,2),cr_return_amt_inc_tax decimal(7,2),cr_fee decimal(7,2),cr_return_ship_cost decimal(7,2),cr_refunded_cash decimal(7,2),cr_reversed_charge decimal(7,2),cr_store_credit decimal(7,2),cr_net_loss decimal(7,2)"},
{"catalog_sales", "cs_sold_date_sk integer,cs_sold_time_sk integer,cs_ship_date_sk integer,cs_bill_customer_sk integer,cs_bill_cdemo_sk integer,cs_bill_hdemo_sk integer,cs_bill_addr_sk integer,cs_ship_customer_sk integer,cs_ship_cdemo_sk integer,cs_ship_hdemo_sk integer,cs_ship_addr_sk integer,cs_call_center_sk integer,cs_catalog_page_sk integer,cs_ship_mode_sk integer,cs_warehouse_sk integer,cs_item_sk integer not null,cs_promo_sk integer,cs_order_number integer not null,cs_quantity integer,cs_wholesale_cost decimal(7,2),cs_list_price decimal(7,2),cs_sales_price decimal(7,2),cs_ext_discount_amt decimal(7,2),cs_ext_sales_price decimal(7,2),cs_ext_wholesale_cost decimal(7,2),cs_ext_list_price decimal(7,2),cs_ext_tax decimal(7,2),cs_coupon_amt decimal(7,2),cs_ext_ship_cost decimal(7,2),cs_net_paid decimal(7,2),cs_net_paid_inc_tax decimal(7,2),cs_net_paid_inc_ship decimal(7,2),cs_net_paid_inc_ship_tax decimal(7,2),cs_net_profit decimal(7,2)"},
{"customer_address", "ca_address_sk integer not null,ca_address_id char(16) not null,ca_street_number char(10),ca_street_name varchar(60),ca_street_type char(15),ca_suite_number char(10),ca_city varchar(60),ca_county varchar(30),ca_state char(2),ca_zip char(10),ca_country varchar(20),ca_gmt_offset decimal(5,2),ca_location_type char(20)"},
{"customer", "c_customer_sk integer not null,c_customer_id char(16) not null,c_current_cdemo_sk integer,c_current_hdemo_sk integer,c_current_addr_sk integer,c_first_shipto_date_sk integer,c_first_sales_date_sk integer,c_salutation char(10),c_first_name char(20),c_last_name char(30),c_preferred_cust_flag char(1),c_birth_day integer,c_birth_month integer,c_birth_year integer,c_birth_country varchar(20),c_login char(13),c_email_address char(50),c_last_review_date char(10)"},
{"customer_demographics", "cd_demo_sk integer not null,cd_gender char(1),cd_marital_status char(1),cd_education_status char(20),cd_purchase_estimate integer,cd_credit_rating char(10),cd_dep_count integer,cd_dep_employed_count integer,cd_dep_college_count integer"},
{"date_dim", "d_date_sk integer not null,d_date_id char(16) not null,d_date date,d_month_seq integer,d_week_seq integer,d_quarter_seq integer,d_year integer,d_dow integer,d_moy integer,d_dom integer,d_qoy integer,d_fy_year integer,d_fy_quarter_seq integer,d_fy_week_seq integer,d_day_name char(9),d_quarter_name char(6),d_holiday char(1),d_weekend char(1),d_following_holiday char(1),d_first_dom integer,d_last_dom integer,d_same_day_ly integer,d_same_day_lq integer,d_current_day char(1),d_current_week char(1),d_current_month char(1),d_current_quarter char(1),d_current_year char(1)"},
{"household_demographics", "hd_demo_sk integer not null,hd_income_band_sk integer,hd_buy_potential char(15),hd_dep_count integer,hd_vehicle_count integer"},
{"income_band", "ib_income_band_sk integer not null,ib_lower_bound integer,ib_upper_bound integer"},
{"inventory", "inv_date_sk integer not null,inv_item_sk integer not null,inv_warehouse_sk integer not null,inv_quantity_on_hand integer"},
{"item", "i_item_sk integer not null,i_item_id char(16) not null,i_rec_start_date date,i_rec_end_date date,i_item_desc varchar(200),i_current_price decimal(7,2),i_wholesale_cost decimal(7,2),i_brand_id integer,i_brand char(50),i_class_id integer,i_class char(50),i_category_id integer,i_category char(50),i_manufact_id integer,i_manufact char(50),i_size char(20),i_formulation char(20),i_color char(20),i_units char(10),i_container char(10),i_manager_id integer,i_product_name char(50)"},
{"promotion", "p_promo_sk integer not null,p_promo_id char(16) not null,p_start_date_sk integer,p_end_date_sk integer,p_item_sk integer,p_cost decimal(15,2),p_response_target integer,p_promo_name char(50),p_channel_dmail char(1),p_channel_email char(1),p_channel_catalog char(1),p_channel_tv char(1),p_channel_radio char(1),p_channel_press char(1),p_channel_event char(1),p_channel_demo char(1),p_channel_details varchar(100),p_purpose char(15),p_discount_active char(1)"},
{"reason", "r_reason_sk integer not null,r_reason_id char(16) not null,r_reason_desc char(100)"},
{"ship_mode", "sm_ship_mode_sk integer not null,sm_ship_mode_id char(16) not null,sm_type char(30),sm_code char(10),sm_carrier char(20),sm_contract char(20)"},
{"store_returns", "sr_returned_date_sk integer,sr_return_time_sk integer,sr_item_sk integer not null,sr_customer_sk integer,sr_cdemo_sk integer,sr_hdemo_sk integer,sr_addr_sk integer,sr_store_sk integer,sr_reason_sk integer,sr_ticket_number integer not null,sr_return_quantity integer,sr_return_amt decimal(7,2),sr_return_tax decimal(7,2),sr_return_amt_inc_tax decimal(7,2),sr_fee decimal(7,2),sr_return_ship_cost decimal(7,2),sr_refunded_cash decimal(7,2),sr_reversed_charge decimal(7,2),sr_store_credit decimal(7,2),sr_net_loss decimal(7,2)"},
{"store_sales", "ss_sold_date_sk integer,ss_sold_time_sk integer,ss_item_sk integer not null,ss_customer_sk integer,ss_cdemo_sk integer,ss_hdemo_sk integer,ss_addr_sk integer,ss_store_sk integer,ss_promo_sk integer,ss_ticket_number integer not null,ss_quantity integer,ss_wholesale_cost decimal(7,2),ss_list_price decimal(7,2),ss_sales_price decimal(7,2),ss_ext_discount_amt decimal(7,2),ss_ext_sales_price decimal(7,2),ss_ext_wholesale_cost decimal(7,2),ss_ext_list_price decimal(7,2),ss_ext_tax decimal(7,2),ss_coupon_amt decimal(7,2),ss_net_paid decimal(7,2),ss_net_paid_inc_tax decimal(7,2),ss_net_profit decimal(7,2)"},
{"store", "s_store_sk integer not null,s_store_id char(16) not null,s_rec_start_date date,s_rec_end_date date,s_closed_date_sk integer,s_store_name varchar(50),s_number_employees integer,s_floor_space integer,s_hours char(20),s_manager varchar(40),s_market_id integer,s_geography_class varchar(100),s_market_desc varchar(100),s_market_manager varchar(40),s_division_id integer,s_division_name varchar(50),s_company_id integer,s_company_name varchar(50),s_street_number varchar(10),s_street_name varchar(60),s_street_type char(15),s_suite_number char(10),s_city varchar(60),s_county varchar(30),s_state char(2),s_zip char(10),s_country varchar(20),s_gmt_offset decimal(5,2),s_tax_precentage decimal(5,2)"},
{"time_dim", "t_time_sk integer not null,t_time_id char(16) not null,t_time integer,t_hour integer,t_minute integer,t_second integer,t_am_pm char(2),t_shift char(20),t_sub_shift char(20),t_meal_time char(20)"},
{"warehouse", "w_warehouse_sk integer not null,w_warehouse_id char(16) not null,w_warehouse_name varchar(20),w_warehouse_sq_ft integer,w_street_number char(10),w_street_name varchar(60),w_street_type char(15),w_suite_number char(10),w_city varchar(60),w_county varchar(30),w_state char(2),w_zip char(10),w_country varchar(20),w_gmt_offset decimal(5,2)"},
{"web_page", "wp_web_page_sk integer not null,wp_web_page_id char(16) not null,wp_rec_start_date date,wp_rec_end_date date,wp_creation_date_sk integer,wp_access_date_sk integer,wp_autogen_flag char(1),wp_customer_sk integer,wp_url varchar(100),wp_type char(50),wp_char_count integer,wp_link_count integer,wp_image_count integer,wp_max_ad_count integer"},
{"web_returns", "wr_returned_date_sk integer,wr_returned_time_sk integer,wr_item_sk integer not null,wr_refunded_customer_sk integer,wr_refunded_cdemo_sk integer,wr_refunded_hdemo_sk integer,wr_refunded_addr_sk integer,wr_returning_customer_sk integer,wr_returning_cdemo_sk integer,wr_returning_hdemo_sk integer,wr_returning_addr_sk integer,wr_web_page_sk integer,wr_reason_sk integer,wr_order_number integer not null,wr_return_quantity integer,wr_return_amt decimal(7,2),wr_return_tax decimal(7,2),wr_return_amt_inc_tax decimal(7,2),wr_fee decimal(7,2),wr_return_ship_cost decimal(7,2),wr_refunded_cash decimal(7,2),wr_reversed_charge decimal(7,2),wr_account_credit decimal(7,2),wr_net_loss decimal(7,2)"},
{"web_sales", "ws_sold_date_sk integer,ws_sold_time_sk integer,ws_ship_date_sk integer,ws_item_sk integer not null,ws_bill_customer_sk integer,ws_bill_cdemo_sk integer,ws_bill_hdemo_sk integer,ws_bill_addr_sk integer,ws_ship_customer_sk integer,ws_ship_cdemo_sk integer,ws_ship_hdemo_sk integer,ws_ship_addr_sk integer,ws_web_page_sk integer,ws_web_site_sk integer,ws_ship_mode_sk integer,ws_warehouse_sk integer,ws_promo_sk integer,ws_order_number integer not null,ws_quantity integer,ws_wholesale_cost decimal(7,2),ws_list_price decimal(7,2),ws_sales_price decimal(7,2),ws_ext_discount_amt decimal(7,2),ws_ext_sales_price decimal(7,2),ws_ext_wholesale_cost decimal(7,2),ws_ext_list_price decimal(7,2),ws_ext_tax decimal(7,2),ws_coupon_amt decimal(7,2),ws_ext_ship_cost decimal(7,2),ws_net_paid decimal(7,2),ws_net_paid_inc_tax decimal(7,2),ws_net_paid_inc_ship decimal(7,2),ws_net_paid_inc_ship_tax decimal(7,2),ws_net_profit decimal(7,2)"},
{"web_site", "web_site_sk integer not null,web_site_id char(16) not null,web_rec_start_date date,web_rec_end_date date,web_name varchar(50),web_open_date_sk integer,web_close_date_sk integer,web_class varchar(50),web_manager varchar(40),web_mkt_id integer,web_mkt_class varchar(50),web_mkt_desc varchar(100),web_market_manager varchar(40),web_company_id integer,web_company_name char(50),web_street_number char(10),web_street_name varchar(60),web_street_type char(15),web_suite_number char(10),web_city varchar(60),web_county varchar(30),web_state char(2),web_zip char(10),web_country varchar(20),web_gmt_offset decimal(5,2),web_tax_percentage decimal(5,2)"}
};

std::string get_columns_type(const std::string& file_name) {
    for (const auto& item : tpcds_tbl_columns) {
        size_t prefix_size = item.first.size();
        if (file_name.compare(0, prefix_size, item.first) != 0) {
            continue;
        }
        if (file_name.size() < prefix_size + 2) {
            continue;
        }
        if (file_name[prefix_size] == '_' && std::isdigit(file_name[prefix_size + 1])) {
            std::cout << "found table:" << item.first << ", for input file:" << file_name << std::endl;
            return item.second;
        }
    }
    return "";
}

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is a CSV to Parquet Converter\n";
    ss << "Usage: \n";
    ss << progname << "--input_file=csv_file --output_file=parquet_file "
          "--columns_type=INT,INT\n";
    return ss.str();
}

class CsvToParquetTool {
public:
    bool init(const std::filesystem::path& root_path) {
        std::string doris_home(root_path.string());
        if (setenv("DORIS_HOME", doris_home.c_str(),  true) != 0) {
            std::cout << "setenv for DORIS_HOME failed" << std::endl;
            return false;
        }
        if (!doris::config::init(nullptr, true, true, true)) {
            LOG(FATAL) << "doris::config::init failed";
            return false;
        }
        LOG(INFO) << "doris::config::init success";

        auto env = doris::ExecEnv::GetInstance();
        std::vector<doris::StorePath> paths = {{doris_home, -1}};
        if (!doris::ExecEnv::init(env, paths).ok()) {
            std::cout << "doris::ExecEnv::init failed" << std::endl;
            return false;
        }
        LOG(INFO) << "doris::ExecEnv::init success";

        _profile = std::make_unique<RuntimeProfile>("CsvToParquetTool");
        doris::TUniqueId id;
        id.hi = 1;
        id.lo = 2;
        doris::TQueryOptions opts;
        doris::TQueryGlobals g;
        _state = std::make_shared<doris::RuntimeState>(id, opts, g, env);
        return true;
    }
    void destroy_env() {
        auto env = doris::ExecEnv::GetInstance();
        doris::ExecEnv::destroy(env);
    }

    void init_block(doris::vectorized::Block* block) {
        for (const auto& tuple : _desc_tbl->get_tuple_descs()) {
            for (const auto& slot : tuple->slots()) {
                doris::vectorized::DataTypePtr data_type = doris::vectorized::DataTypeFactory::instance().create_data_type(slot->type(), slot->is_nullable());
                doris::vectorized::MutableColumnPtr data_column = data_type->create_column();
                block->insert(doris::vectorized::ColumnWithTypeAndName(std::move(data_column), data_type, slot->col_name()));
            }
        }
    }

    ~CsvToParquetTool() {
        destroy_env();
    }

    // decimal(7,2) or char(10) or varchar(20)
    bool get_sub_nums(const std::string& input, std::vector<int>& result) {
        size_t start = input.find_first_of('(');
        size_t end = input.find_first_of(')');
        if (start == std::string::npos || end == std::string::npos || end <= start) {
            LOG(WARNING) << "invalid input for get_sub_nums, input=" << input;
            return false;
        }
        std::vector<std::string> elems;
        doris::split_string<char>(input.substr(start + 1, end - start - 1), ',', &elems);
        result.clear();
        for (const auto& elem : elems) {
            //LOG(INFO) << "input[" << input << "] --> " << std::stoi(elem);
            result.push_back(std::stoi(elem));
        }
        return true;
    }
    bool build_slot(TSlotDescriptorBuilder& builder, const std::string& type) {
        std::vector<std::string> elems;
        doris::split_string<char>(type, ' ', &elems);
        for (int i = 1; i < elems.size(); ++i) {
            std::transform(elems[i].begin(), elems[i].end(), elems[i].begin(), [](unsigned char c) {return std::toupper(c);});
        }
        // case 1: column_name column_type
        // case 2: column_name column_type NULL
        // case 3: column_name column_type NOT NULL
        if (elems.size() > 4 || elems.size() < 2) {
            LOG(WARNING) << "invalid input type: " << type;
            return false;
        }
        if (elems.size() >= 3 && elems.back() != "NULL") {
            LOG(WARNING) << "invalid input type: " << type;
            return false;
        }
        if (elems.size() >= 4 && elems[2] != "NOT") {
            LOG(WARNING) << "invalid input type: " << type;
            return false;
        }
        builder.column_name(elems[0]);
        builder.nullable(elems.size() != 4);

        std::vector<int> sub_infos;
        std::string col_name = elems[1];
        if (col_name == "INT") {
            builder.type(doris::TYPE_INT);
        } else if (col_name == "BIGINT" || col_name == "INTEGER") {
            builder.type(doris::TYPE_BIGINT);
        } else if (col_name == "DATE") {
            builder.type(doris::TYPE_DATE);
        } else if (col_name == "DATETIME") {
            builder.type(doris::TYPE_DATETIME);
        } else if (col_name.compare(0, 7, "DECIMAL") == 0 || col_name.compare(0, 9, "DECIMALV3") == 0) {
            if (!get_sub_nums(col_name, sub_infos) || sub_infos.size() != 2) {
                LOG(WARNING) << "invalid input type: " << type;
                return false;
            }
            builder.decimal_type(sub_infos[0], sub_infos[1]);
        } else if (col_name.compare(0, 4, "CHAR") == 0 || col_name.compare(0, 7, "VARCHAR") == 0) {
            if (!get_sub_nums(col_name, sub_infos) || sub_infos.size() != 1) {
                LOG(WARNING) << "invalid input type: " << type;
                return false;
            }
            builder.string_type(sub_infos[0]);
        } else {
            LOG(WARNING) << "not supported type: " << type;
            return false;
        }
        return true;
    }
    bool build_tbl_schema() {
        std::string columns_type(FLAGS_columns_type);
        if (columns_type.empty()) {
            LOG(WARNING) << "MUST input columns_type for each column";
            return false;
        }

        std::vector<std::string> types;
        size_t offset = 0;
        while (offset < columns_type.size()) {
            size_t next = columns_type.find(',', offset);
            if (next == std::string::npos) {
                types.push_back(columns_type.substr(offset));
                break;
            }
            // for decimal(7,2)
            if (next + 1 < columns_type.size() && std::isdigit(columns_type[next + 1])) {
                next = columns_type.find(',', next + 1);
            }
            if (next == std::string::npos) {
                types.push_back(columns_type.substr(offset));
                break;
            } else {
                types.push_back(columns_type.substr(offset, next - offset));
                offset = next + 1;
            }
        }

        doris::TDescriptorTableBuilder dtb;
        doris::TTupleDescriptorBuilder tuple_builder;
        for (size_t i = 0; i < types.size(); ++i) {
            TSlotDescriptorBuilder slot_builder;
            slot_builder.column_pos(i);
            if (!build_slot(slot_builder, types[i])) {
                LOG(WARNING) << "build_slot failed for type:" << types[i];
                return false;
            }
            tuple_builder.add_slot(slot_builder.build());
        }
        tuple_builder.build(&dtb);
        doris::TDescriptorTable tdesc_tbl = dtb.desc_tbl();

        doris::DescriptorTbl::create(&_obj_pool, tdesc_tbl, &_desc_tbl);
        if (!_desc_tbl) {
            LOG(WARNING) << "DescriptorTbl::create failed";
            return false;
        }
        LOG(INFO) << "schema is:" << _desc_tbl->debug_string();
        return true;
    }

    std::unique_ptr<doris::vectorized::CsvReader> create_csv_reader(const std::filesystem::path& file) {
        _counter = std::make_shared<doris::vectorized::ScannerCounter>();

        _params.format_type = doris::TFileFormatType::FORMAT_CSV_PLAIN;
        _params.file_attributes.text_params.column_separator = FLAGS_column_separator;
        _params.file_attributes.text_params.line_delimiter = FLAGS_line_delimiter;

        _range.path = file.string();
        _range.size = -1;

        for (const auto& tuple : _desc_tbl->get_tuple_descs()) {
            for (const auto& slot : tuple->slots()) {
                _file_slots.push_back(slot);
            }
        }

        return CsvReader::create_unique(_state.get(), _profile.get(), _counter.get(), _params, _range, _file_slots, &_io_ctx);
    }

    bool build_parquet_schema() {
        _parquet_schemas.clear();
        for (const auto& tuple : _desc_tbl->get_tuple_descs()) {
            for (const auto& slot : tuple->slots()) {
                doris::TParquetSchema col;
                if (slot->is_nullable()) {
                    col.__set_schema_repetition_type(doris::TParquetRepetitionType::OPTIONAL);
                } else {
                    col.__set_schema_repetition_type(doris::TParquetRepetitionType::REQUIRED);
                }
                col.__set_schema_column_name(slot->col_name());
                if (slot->type().type == doris::PrimitiveType::TYPE_BOOLEAN) {
                    col.__set_schema_data_type(doris::TParquetDataType::BOOLEAN);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_INT || slot->type().type == doris::PrimitiveType::TYPE_DATE) {
                    col.__set_schema_data_type(doris::TParquetDataType::INT32);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_BIGINT || slot->type().type == doris::PrimitiveType::TYPE_DATETIME) {
                    col.__set_schema_data_type(doris::TParquetDataType::INT64);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_DECIMAL32 || slot->type().type == doris::PrimitiveType::TYPE_DECIMAL64 || slot->type().type == doris::PrimitiveType::TYPE_DECIMALV2) {
                    col.__set_schema_data_type(doris::TParquetDataType::FIXED_LEN_BYTE_ARRAY);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_STRING || slot->type().type == doris::PrimitiveType::TYPE_VARCHAR || slot->type().type == doris::PrimitiveType::TYPE_CHAR) {
                    col.__set_schema_data_type(doris::TParquetDataType::BYTE_ARRAY);
                } else {
                    LOG(INFO) << "do not supported slot type:" << slot->debug_string();
                    return false;
                }

                if (slot->type().type == doris::PrimitiveType::TYPE_DATE) {
                    col.__set_schema_data_logical_type(doris::TParquetDataLogicalType::DATE);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_DATETIME) {
                    col.__set_schema_data_logical_type(doris::TParquetDataLogicalType::TIMESTAMP);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_DECIMAL32 || slot->type().type == doris::PrimitiveType::TYPE_DECIMAL64 || slot->type().type == doris::PrimitiveType::TYPE_DECIMALV2) {
                    col.__set_schema_data_logical_type(doris::TParquetDataLogicalType::DECIMAL);
                } else {
                    col.__set_schema_data_logical_type(doris::TParquetDataLogicalType::NONE);
                }
                _parquet_schemas.push_back(col);
            }
        }
        return true;
    }

    bool build_output_exprs() {
        _output_vexpr_ctxs.clear();
        std::vector<doris::TExpr> exprs;
        for (const auto& tuple : _desc_tbl->get_tuple_descs()) {
            for (const auto& slot : tuple->slots()) {
                doris::TScalarType tscalar_type;
                tscalar_type.__set_type(doris::to_thrift(slot->type().type));
                if (slot->type().type == doris::PrimitiveType::TYPE_STRING || slot->type().type == doris::PrimitiveType::TYPE_VARCHAR || slot->type().type == doris::PrimitiveType::TYPE_CHAR) {
                    tscalar_type.__set_len(slot->type().len);
                } else if (slot->type().type == doris::PrimitiveType::TYPE_DECIMAL32 || slot->type().type == doris::PrimitiveType::TYPE_DECIMAL64 || slot->type().type == doris::PrimitiveType::TYPE_DECIMAL128I) {
                    tscalar_type.__set_precision(slot->type().precision);
                    tscalar_type.__set_scale(slot->type().scale);
                }

                doris::TTypeNode ttype_node;
                ttype_node.__set_type(doris::TTypeNodeType::SCALAR);
                ttype_node.__set_scalar_type(tscalar_type);

                doris::TTypeDesc t_type_desc;
                t_type_desc.types.push_back(ttype_node);
                t_type_desc.__set_is_nullable(slot->is_nullable());

                doris::TExprNode node;
                node.__set_type(t_type_desc);
                node.__set_node_type(doris::TExprNodeType::COLUMN_REF);
                node.__set_num_children(0);

                doris::TExpr expr;
                expr.nodes.push_back(node);

                exprs.push_back(expr);
            }
        }
        if (!doris::vectorized::VExpr::create_expr_trees(exprs, _output_vexpr_ctxs).ok()) {
            LOG(WARNING) << "VExpr::create_expr_trees failed";
            return false;
        }
        return true;
    }

    std::unique_ptr<doris::vectorized::VParquetWriterWrapper> create_parquet_writer(const std::filesystem::path& output_file) {
        if (!doris::io::global_local_filesystem()->create_file(output_file.string(), &_local_file_writer).ok()) {
            LOG(WARNING) << "create output file: " << output_file << " failed";
            return nullptr;
        }

        if (!build_parquet_schema()) {
            LOG(WARNING) << "build_parquet_schema failed";
            return nullptr;
        }

        if (!build_output_exprs()) {
            LOG(WARNING) << "build_output_exprs failed";
            return nullptr;
        }

/*
        doris::TExprNode node;
        doris::TScalarType tscalar_type;
        tscalar_type.__set_type(doris::TPrimitiveType::INT);
        doris::TTypeNode ttype_node;
        ttype_node.__set_type(doris::TTypeNodeType::SCALAR);
        ttype_node.__set_scalar_type(tscalar_type);
        doris::TTypeDesc t_type_desc;
        t_type_desc.types.push_back(ttype_node);
        t_type_desc.__set_is_nullable(true);
        node.__set_type(t_type_desc);
        node.__set_node_type(doris::TExprNodeType::COLUMN_REF);
        node.__set_num_children(0);

        doris::TExpr expr1;
        expr1.nodes.push_back(node);
        doris::TExpr expr2;
        expr2.nodes.push_back(node);
        std::vector<doris::TExpr> exprs;
        exprs.push_back(expr1);
        exprs.push_back(expr2);
        if (!doris::vectorized::VExpr::create_expr_trees(exprs, _output_vexpr_ctxs).ok()) {
            LOG(WARNING) << "VExpr::create_expr_trees failed";
            return nullptr;
        }
*/

        doris::TParquetCompressionType::type compression_type = doris::TParquetCompressionType::SNAPPY;
        bool parquet_disable_dictionary = false;
        doris::TParquetVersion::type parquet_version = doris::TParquetVersion::PARQUET_2_LATEST;
        bool output_object_data = false;
        return std::make_unique<doris::vectorized::VParquetWriterWrapper>(_local_file_writer.get(), _output_vexpr_ctxs, _parquet_schemas, compression_type, parquet_disable_dictionary, parquet_version, output_object_data);
    }

    bool convert(const std::filesystem::path& input_file, const std::filesystem::path& output_file) {
        LOG(INFO) << "begin to convert: " << input_file.string() << " to " << output_file.string();

        if (!build_tbl_schema()) {
            LOG(WARNING) << "build_tbl_schema failed";
            return false;
        }

        auto reader = create_csv_reader(input_file);
        if (!reader) {
            LOG(WARNING) << "create_csv_reader failed";
            return false;
        }
        if (!reader->init_reader(true).ok()) {
            LOG(WARNING) << "reader->init_reader failed";
            return false;
        }
        LOG(INFO) << "init_reader success";

        auto writer = create_parquet_writer(output_file);
        if (!writer) {
            LOG(WARNING) << "create_parquet_writer failed";
            return false;
        }
        if (!writer->prepare().ok()) {
            LOG(WARNING) << "writer->prepare() failed";
            return false;
        }
        LOG(INFO) << "init_writer success";

        doris::vectorized::Block block;
        init_block(&block);
        size_t total_read_rows = 0;
        size_t read_rows = 0;
        bool eof = false;
        doris::Status ret;
        while ((ret = reader->get_next_block(&block, &read_rows, &eof)).ok()) {
            if (_counter->num_rows_filtered > 0) {
                LOG(WARNING) << "some rows are filtered, num_rows_filtered=" << _counter->num_rows_filtered;
                return false;
            }
            if (eof) {
                LOG(INFO) << "reach end of reader->get_next_block";
                break;
            }
            //LOG(INFO) << "read rows=" << read_rows << ", data=" << block.dump_data();
            total_read_rows += read_rows;
            //LOG(INFO) << "total read rows=" << total_read_rows << ", this batch read rows=" << read_rows << ", block.rows=" << block.rows();

            if (!(ret = writer->write(block)).ok()) {
                LOG(WARNING) << "writer->write failed";
                break;
            }
            //LOG(INFO) << "write rows=" << read_rows << ", data=" << block.dump_data();
            block.clear_column_data();
            if (total_read_rows >= FLAGS_max_output_lines) {
                break;
            }
        }
        if (!ret.ok()) {
            LOG(WARNING) << "reader->get_next_block failed";
            return false;
        }

        if (!writer->close().ok()) {
            LOG(WARNING) << "writer->close() failed";
            return false;
        }
        LOG(INFO) << "convert [" << input_file.string() << "] to [" << output_file.string() << "] success total_rows=" << total_read_rows;
        return true;
    }
private:
    std::unique_ptr<RuntimeProfile> _profile;
    std::shared_ptr<doris::RuntimeState> _state;
    std::shared_ptr<doris::vectorized::ScannerCounter> _counter;

    doris::TFileScanRangeParams _params;
    doris::TFileRangeDesc _range;
    std::vector<SlotDescriptor*> _file_slots;
    doris::io::IOContext _io_ctx;

    doris::ObjectPool _obj_pool;
    doris::DescriptorTbl* _desc_tbl;

    doris::io::FileWriterPtr _local_file_writer;
    doris::vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    std::vector<doris::TParquetSchema> _parquet_schemas;
};

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    // check input file
    if (FLAGS_input_file.empty()) {
        std::cout << "No input file.\n" << std::endl << usage;
        return -1;
    }
    std::filesystem::path input_file(FLAGS_input_file);
    input_file = std::filesystem::absolute(input_file);
    if (!std::filesystem::is_regular_file(input_file)) {
        std::cout << "input file:" << input_file.string() << " do not exist.\n" << std::endl << usage;
        return -1;
    }
    std::string columns_type = get_columns_type(input_file.filename());
    if (!columns_type.empty()) {
        FLAGS_columns_type = columns_type;
        LOG(WARNING) << "found registed table, replace with new colums type:" << FLAGS_columns_type;
    }

    // check output file
    if (FLAGS_output_file.empty()) {
        std::cout << "No output file.\n" << std::endl << usage;
        return -1;
    }
    std::filesystem::path output_file(FLAGS_output_file);
    output_file = std::filesystem::absolute(output_file);

    // use output dir as root path
    std::filesystem::path root_path = output_file.parent_path();
    if (!std::filesystem::is_directory(root_path)) {
        std::cout << "output dir:" << root_path.string() << " do not exist.\n" << std::endl << usage;
        return -1;
    }

    // convert csv file to parquet file
    CsvToParquetTool tool;
    if (!tool.init(root_path)) {
        std::cout << "CsvToParquetTool::init failed, root_path=" << root_path.string() << std::endl;
        return -1;
    }
    if (!tool.convert(input_file, output_file)) {
        std::cout << "CsvToParquetTool::convert failed" << std::endl;
        return -1;
    }

    return 0;
}
