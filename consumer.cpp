#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#ifndef _MSC_VER
#include <sys/time.h>
#endif

#ifdef _MSC_VER
#include <atltime.h>
#elif _AIX
#include <unistd.h>
#else
#include <getopt.h>
#include <unistd.h>
#endif

#include "rdkafkacpp.h"
#include "rdkafka.h"
#include <iostream>
#include <opencv2/highgui/highgui.hpp>

#include "rdkafkacpp.h"

#define PARTITION_0 0
#define PARTITION_1 1
#define PARTITION_2 2 
#define PARTITION_3 3
#define PARTITION_3 3
#define PARTITION_4 4
#define PARTITION_5 5
#define PARTITION_6 6 
#define PARTITION_7 7
#define PARTITION_8 8
#define PARTITION_9 9

static bool run = true;
static bool exit_eof = false;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 1;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
const int batch_size = 2;
const std::string topic_str = "LINGVOv93";

static void sigterm(int sig) {
	run = false;
}

/**
 * @brief format a string timestamp from the current time
 */

static void print_time() {
#ifndef _MSC_VER
	struct timeval tv;
	char buf[64];
	gettimeofday(&tv, NULL);
	strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
	fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
#else
	std::wcerr << CTime::GetCurrentTime().Format(_T("%Y-%m-%d %H:%M:%S")).GetString()
		<< ": ";
#endif
}
class ExampleEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) 
    {
        print_time();
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
            if (event.err()) 
            {
                std::cerr << "FATAL ";
                run = false;
            }
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;

        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(), event.str().c_str());
            break;

        case RdKafka::Event::EVENT_THROTTLE:
            std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " << event.broker_name() << " id " << (int)event.broker_id() << std::endl;
            break;

        default:
            std::cerr << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
            break;
        }
    }
};

class ExampleRebalanceCb : public RdKafka::RebalanceCb {
private:
	static void part_list_print(const std::vector<RdKafka::TopicPartition*>&partitions) 
        {
            std::cout << "PAR " << partitions.size() << std::endl;
            for (unsigned int i = 0; i < partitions.size(); i++){ std::cerr << partitions[i]->topic() <<"[" << partitions[i]->partition() << "], "; }
            std::cerr << "\n";
	}

public:
	void rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*> &partitions) 
        {
            std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

            part_list_print(partitions);
            if (err == RdKafka::ERR__ASSIGN_PARTITIONS) 
            {
                //partitions.push_back(RdKafka::TopicPartition::create(topic_str, 0, 0));
                consumer->assign(partitions);
                partition_cnt = (int)partitions.size();
                std::cout << "PAR " << partitions.size() << std::endl;
                std::cout << "PARTITION CNT " << partition_cnt << std::endl;
            }
            else 
            {
                consumer->unassign();
                partition_cnt = 0;
            }
            eof_cnt = 0;
	}
};

void msg_consume(RdKafka::Message* message, void* opaque) {
	switch (message->err()) {
	case RdKafka::ERR__TIMED_OUT:
		break;

	case RdKafka::ERR_NO_ERROR:
		/* Real message */
		msg_cnt++;
		msg_bytes += message->len();
		if (verbosity >= 3)
			std::cerr << "Read msg at offset " << message->offset() << std::endl;
		RdKafka::MessageTimestamp ts;
		ts = message->timestamp();
		if (verbosity >= 2 &&
			ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
			std::string tsname = "?";
			if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
				tsname = "create time";
			else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
				tsname = "log append time";
			std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
		}
		if (verbosity >= 2 && message->key()) {
			std::cout << "Key: " << *message->key() << std::endl;
		}
		//if (verbosity >= 1) {
			//printf("%.*s\n",
				//static_cast<int>(message->len()),
				//static_cast<const char *>(message->payload()));
                    //std::cout << "MSG" << std::endl;
		//}
		break;

	case RdKafka::ERR__PARTITION_EOF:
		/* Last message */
		if (exit_eof && ++eof_cnt == partition_cnt) {
			std::cerr << "%% EOF reached for all " << partition_cnt <<
				" partition(s)" << std::endl;
			run = false;
		}
		break;

	case RdKafka::ERR__UNKNOWN_TOPIC:
		break;
	case RdKafka::ERR__UNKNOWN_PARTITION:
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
		break;

	default:
		/* Errors */
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
	}
}


class ExampleConsumeCb : public RdKafka::ConsumeCb {
 public:
  void consume_cb (RdKafka::Message &msg, void *opaque) 
  {
    msg_consume(&msg, opaque);
  }
};

void conf_tcong_generation(RdKafka::Conf *conf, RdKafka::Conf *tconf, int partition_id, std::string errstr)
{
    std::string broker_a = "35.231.213.214:9094";
    std::string broker_b = "35.231.213.214:9095";
    conf->set("enable.partition.eof", "false", errstr);
    if ((conf->set("group.id", "consumer", errstr) != RdKafka::Conf::CONF_OK)) 
    {
        std::cerr << errstr << "1" << std::endl;
        exit(1);
    }
	//The following is valid in producer
	
    if ((conf->set("queue.buffering.max.ms", "0", errstr) != RdKafka::Conf::CONF_OK)) 
    {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    //To minimize end-to-end latency, minimize socket.blocking.max.ms, fetch.wait.max.ms, and fetch.error.backoff.ms
    //Noticed that a low fetch.wait.max.ms will increase network and CPU usage at no gain.
//    if ((conf->set("socket.blocking.max.ms", "1", errstr) != RdKafka::Conf::CONF_OK))
//    {
//            std::cerr << errstr << "1" << std::endl;
//            exit(1);
//    }
    if ((conf->set("fetch.wait.max.ms", "1", errstr) != RdKafka::Conf::CONF_OK))
    {
            std::cerr << errstr << "1" << std::endl;
            exit(1);
    }
    if ((conf->set("fetch.error.backoff.ms", "1", errstr) != RdKafka::Conf::CONF_OK))
    {
            std::cerr << errstr << "1" << std::endl;
            exit(1);
    }
    if ((conf->set("socket.nagle.disable", "true", errstr) != RdKafka::Conf::CONF_OK))
    {
            std::cerr << errstr << "1" << std::endl;
            exit(1);
    }
    if(partition_id == 0 || partition_id == 1 || partition_id == 2 || partition_id == 3 || partition_id == 4)
    {
        conf->set("bootstrap.servers", broker_a, errstr);
    }
    else if(partition_id == 5 || partition_id == 6 || partition_id == 7 || partition_id == 8 || partition_id == 9)
    {
        conf->set("bootstrap.servers", broker_b, errstr);
    }
    else
    {
        std::cout << "INVALID PARTITION ID" << std::endl;
        exit(1);
    }
    ExampleEventCb ex_event_cb;
    conf->set("event_cb", &ex_event_cb, errstr);
    conf->set("default_topic_conf", tconf, errstr);
}

void consumer_handler_generation_validation(RdKafka::Consumer *consumer, std::string errstr)
{
    if (!consumer) 
    {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        exit(1);
    }
    std::cout << "% Created consumer " << consumer->name() << std::endl;
}

void topic_handler_generation_validation(RdKafka::Topic *topic, std::string errstr)
{
     if (!topic) 
    {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        exit(1);
    }
}

void start_handler_generation_validation(RdKafka::ErrorCode resp)
{
    if (resp != RdKafka::ERR_NO_ERROR) 
    {
        std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
        exit(1);
    }
}


int main(int argc, char **argv) 
{
    std::string errstr0;
    std::string errstr1;
    std::string errstr2;
    std::string errstr3;
    std::string errstr4;
    std::string errstr5;
    std::string errstr6;
    std::string errstr7;
    std::string errstr8;
    std::string errstr9;
    
    std::string mode;
    std::string debug;
    std::vector<std::string> topics;
    topics.push_back(topic_str);

    int32_t partition = 1; 
    int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
    
    int counter_frames = 0;
    int consume_timeout = 1000;
    
    /*
     * Create configuration objects
     */
    RdKafka::Conf *conf0 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf0 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf0, tconf0, PARTITION_0, errstr0);
    
    RdKafka::Conf *conf1 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf1 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf1, tconf1, PARTITION_1, errstr1);
    
    RdKafka::Conf *conf2 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf2 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf2, tconf2, PARTITION_2, errstr2);
    
    RdKafka::Conf *conf3 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf3 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf3, tconf3, PARTITION_3, errstr3);
    
    RdKafka::Conf *conf4 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf4 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf4, tconf4, PARTITION_4, errstr4);
    
    RdKafka::Conf *conf5 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf5 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf5, tconf5, PARTITION_5, errstr5);
    
    RdKafka::Conf *conf6 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf6 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf6, tconf6, PARTITION_6, errstr6);
    
    RdKafka::Conf *conf7 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf7 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf7, tconf7, PARTITION_7, errstr7);
    
    RdKafka::Conf *conf8 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf8 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf8, tconf8, PARTITION_8, errstr8);
    
    RdKafka::Conf *conf9 = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf9 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    conf_tcong_generation(conf9, tconf9, PARTITION_9, errstr9);
    
    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);
     
    
    
     /*
     * Consumer mode
     */

    if(topic_str.empty()){return -1;}
    /*
     * Create consumer using accumulated global configuration.
     */
    RdKafka::Consumer *consumer0 = RdKafka::Consumer::create(conf0, errstr0);
    consumer_handler_generation_validation(consumer0, errstr0);
    
    RdKafka::Consumer *consumer1 = RdKafka::Consumer::create(conf1, errstr1);
    consumer_handler_generation_validation(consumer1, errstr1);
    
    RdKafka::Consumer *consumer2 = RdKafka::Consumer::create(conf2, errstr2);
    consumer_handler_generation_validation(consumer2, errstr2);
    
    RdKafka::Consumer *consumer3 = RdKafka::Consumer::create(conf3, errstr3);
    consumer_handler_generation_validation(consumer3, errstr3);
    
    RdKafka::Consumer *consumer4 = RdKafka::Consumer::create(conf4, errstr4);
    consumer_handler_generation_validation(consumer4, errstr4);
    
    RdKafka::Consumer *consumer5 = RdKafka::Consumer::create(conf5, errstr5);
    consumer_handler_generation_validation(consumer5, errstr5);
    
    RdKafka::Consumer *consumer6 = RdKafka::Consumer::create(conf6, errstr6);
    consumer_handler_generation_validation(consumer6, errstr6);
    
    RdKafka::Consumer *consumer7 = RdKafka::Consumer::create(conf7, errstr7);
    consumer_handler_generation_validation(consumer7, errstr7);
    
    RdKafka::Consumer *consumer8 = RdKafka::Consumer::create(conf8, errstr8);
    consumer_handler_generation_validation(consumer8, errstr8);
    
    RdKafka::Consumer *consumer9 = RdKafka::Consumer::create(conf9, errstr9);
    consumer_handler_generation_validation(consumer9, errstr9);
    
    
    /*
     * Create topic handle.
     */
    RdKafka::Topic *topic0 = RdKafka::Topic::create(consumer0, topic_str, tconf0, errstr0);
    topic_handler_generation_validation(topic0, errstr0);
    
    RdKafka::Topic *topic1 = RdKafka::Topic::create(consumer1, topic_str, tconf1, errstr1);
    topic_handler_generation_validation(topic1, errstr1);
    
    RdKafka::Topic *topic2 = RdKafka::Topic::create(consumer2, topic_str, tconf2, errstr2);
    topic_handler_generation_validation(topic2, errstr2);
    
    RdKafka::Topic *topic3 = RdKafka::Topic::create(consumer3, topic_str, tconf3, errstr3);
    topic_handler_generation_validation(topic3, errstr3);
    
    RdKafka::Topic *topic4 = RdKafka::Topic::create(consumer4, topic_str, tconf4, errstr4);
    topic_handler_generation_validation(topic4, errstr4);
    
    RdKafka::Topic *topic5 = RdKafka::Topic::create(consumer5, topic_str, tconf5, errstr5);
    topic_handler_generation_validation(topic5, errstr5);
    
    RdKafka::Topic *topic6 = RdKafka::Topic::create(consumer6, topic_str, tconf6, errstr6);
    topic_handler_generation_validation(topic6, errstr6);
    
    RdKafka::Topic *topic7 = RdKafka::Topic::create(consumer7, topic_str, tconf7, errstr7);
    topic_handler_generation_validation(topic7, errstr7);
    
    RdKafka::Topic *topic8 = RdKafka::Topic::create(consumer8, topic_str, tconf8, errstr8);
    topic_handler_generation_validation(topic8, errstr8);
    
    RdKafka::Topic *topic9 = RdKafka::Topic::create(consumer9, topic_str, tconf9, errstr9);
    topic_handler_generation_validation(topic9, errstr9);

    
    /*
     * Start consumer for topic+partition at start offset
     */
    RdKafka::ErrorCode resp0 = consumer0->start(topic0, PARTITION_0, start_offset);
    start_handler_generation_validation(resp0);
    
    RdKafka::ErrorCode resp1 = consumer1->start(topic1, PARTITION_1, start_offset);
    start_handler_generation_validation(resp1);
    
    RdKafka::ErrorCode resp2 = consumer2->start(topic2, PARTITION_2, start_offset);
    start_handler_generation_validation(resp2);
    
    RdKafka::ErrorCode resp3 = consumer3->start(topic3, PARTITION_3, start_offset);
    start_handler_generation_validation(resp3);
    
    RdKafka::ErrorCode resp4 = consumer4->start(topic4, PARTITION_4, start_offset);
    start_handler_generation_validation(resp4);
    
    RdKafka::ErrorCode resp5 = consumer5->start(topic5, PARTITION_5, start_offset);
    start_handler_generation_validation(resp5);
    
    RdKafka::ErrorCode resp6 = consumer6->start(topic6, PARTITION_6, start_offset);
    start_handler_generation_validation(resp6);
    
    RdKafka::ErrorCode resp7 = consumer7->start(topic7, PARTITION_7, start_offset);
    start_handler_generation_validation(resp7);
    
    RdKafka::ErrorCode resp8 = consumer8->start(topic8, PARTITION_8, start_offset);
    start_handler_generation_validation(resp8);
    
    RdKafka::ErrorCode resp9 = consumer9->start(topic9, PARTITION_9, start_offset);
    start_handler_generation_validation(resp9);

    
    ExampleConsumeCb ex_consume_cb;
    /*
     * Consume messages
     */

    std::vector<uchar> encoded;
    std::vector<bool> consume_part_msg = {false,false,false,false,false,false,false,false,false,false};
    std::vector<int> key_batch = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    std::vector<int> init_partition = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    cv::Mat img_zeros = cv::Mat::zeros(cvSize(1024, 1024), CV_8UC3);
    std::vector<cv::Mat> mat_batch = {img_zeros, img_zeros, img_zeros, img_zeros, img_zeros, img_zeros, img_zeros, img_zeros, img_zeros, img_zeros};
    mat_batch.clear();
    std::string key_value_str;
    int partition_counter = 0;
    long key_status = -1;
    long last_key_status = -1;
    RdKafka::Message *msg;
    bool retry_s = false;
    while (run) 
    {
    retry_init: 
        if(retry_s)
        { 
            partition_counter ++;
            last_key_status ++;
            retry_s = false;
            
        }
        if (partition_counter == 0)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 0
                msg = consumer0->consume(topic0, PARTITION_0, consume_timeout);
            }
        }
        else if (partition_counter == 1)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 1
                msg = consumer1->consume(topic1, PARTITION_1, consume_timeout);
            }
        } 
        else if (partition_counter == 2)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 2
                //std::cout << partition_counter << std::endl;
                msg = consumer2->consume(topic2, PARTITION_2, consume_timeout);
            }
        }
        else if (partition_counter == 3)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 3
                //std::cout << partition_counter << std::endl;
                msg = consumer3->consume(topic3, PARTITION_3, consume_timeout);
            }
        }
        else if (partition_counter == 4)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 4
                msg = consumer4->consume(topic4, PARTITION_4, consume_timeout);
            }
        }
        else if (partition_counter == 5)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 5
                //std::cout << partition_counter << std::endl;
                msg = consumer5->consume(topic5, PARTITION_5, consume_timeout);
            }
        }
        else if (partition_counter == 6)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 6
                //std::cout << partition_counter << std::endl;
                msg = consumer6->consume(topic6, PARTITION_6, consume_timeout);
            }
        }
        else if (partition_counter == 7)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 7
                //std::cout << partition_counter << std::endl;
                msg = consumer7->consume(topic7, PARTITION_7, consume_timeout);
            }
        }
        else if (partition_counter == 8)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 8
                //std::cout << partition_counter << std::endl;
                msg = consumer8->consume(topic8, PARTITION_8, consume_timeout);
            }
        }
        else if (partition_counter == 9)
        {
            if(!consume_part_msg[partition_counter])
            {
                //CONSUME MSG @ PART 9
                //std::cout << partition_counter << std::endl;
                msg = consumer9->consume(topic9, PARTITION_9, consume_timeout);
            }
        }
//    
//        for(auto x : consume_part_msg)
//        {
//            std:: cout << "CONSUME PART: " << x << std::endl;
//        }
//        
//        for(auto x : key_batch)
//        {
//            std:: cout << "KEY BATCH: " << x << std::endl;
//        }
//        cv::waitKey(3000);  
//    
        if(msg != NULL && !consume_part_msg[partition_counter])
        {
            if(msg->len() > 1400 && msg->payload() != NULL)
            {
                char* data_udp = static_cast<char*>(msg->payload());
                std::string string_udp(data_udp, msg->len());
                encoded.insert(encoded.end(), string_udp.begin(), string_udp.end());
                cv::Mat frame_test = cv::imdecode(encoded,CV_LOAD_IMAGE_COLOR);
                key_value_str = *msg->key();
                key_batch[partition_counter] = std::stoi(key_value_str);
                mat_batch[partition_counter] = frame_test.clone();
                encoded.clear();   
                consume_part_msg[partition_counter] = true;
                init_partition[partition_counter] = init_partition[partition_counter] + 1;
                msg = NULL;
                cv::waitKey(34);
            }
            else
            {
                if(last_key_status != -1){ retry_s = true; }
                goto retry_init;
                
            }
        }
        else
        {
            goto retry_init;
        }
        long min = 0;
        bool first = true;
        for(int i = 0; i < key_batch.size(); i++)
        {
            if(key_batch[i] == -1) 
            { 
                i =  key_batch.size();
                min = key_batch.size();
            }
            else if(consume_part_msg[i])
            {
                if(first)
                { 
                    min = i; 
                    first = false;
                }
                else if (key_batch[i] < key_batch[min] && key_batch[i] > last_key_status) 
                { 
                    min = i;
                }
            }
        }
        if(min != key_batch.size()){ key_status = key_batch[min]; }
        std::cout << "LAST " <<  last_key_status << " ACT " << key_status << std::endl;
        //cv::waitKey(2000);
        if(last_key_status < key_status)
        {
            cv::Mat batch_frame_status = mat_batch[min];
            consume_part_msg[min] = false;
            cv::imshow("CONSUMER", batch_frame_status);
            counter_frames++;
            std::cout << "FRAMES: " << counter_frames  << " KEY " << key_status << std::endl;
            cv::waitKey(34);
            last_key_status = key_status;

        }
        if(min == key_batch.size())
        { 
            if(partition_counter != 9){ partition_counter++; }
            else { partition_counter = 0; }
        }
        else { partition_counter = min; }

        consumer0->poll(0);
        consumer1->poll(0);
        consumer2->poll(0);
        consumer3->poll(0);
        consumer4->poll(0);
        consumer5->poll(0);
        consumer6->poll(0);
        consumer7->poll(0);
        consumer8->poll(0);
        consumer9->poll(0);
    }
    delete topic0;
    delete consumer0;
    delete topic1;
    delete consumer1;
    delete topic2;
    delete consumer2;
    delete topic3;
    delete consumer3;
    delete topic4;
    delete consumer4;
    delete topic5;
    delete consumer5;
    delete topic6;
    delete consumer6;
    delete topic7;
    delete consumer7;
    delete topic8;
    delete consumer8;
    delete topic9;
    delete consumer9;

#ifndef _MSC_VER
    alarm(10);
#endif

    /*
     * Stop consumer
     */
    consumer0->stop(topic0, PARTITION_0);
    consumer1->stop(topic1, PARTITION_1);
    consumer2->stop(topic2, PARTITION_2);
    consumer3->stop(topic3, PARTITION_3);
    consumer4->stop(topic4, PARTITION_4);
    consumer5->stop(topic5, PARTITION_5);
    consumer6->stop(topic6, PARTITION_6);
    consumer7->stop(topic7, PARTITION_7);
    consumer8->stop(topic8, PARTITION_8);
    consumer9->stop(topic9, PARTITION_9);
    consumer0->poll(1000);


    std::cerr << "% Consumed " << msg_cnt << " messages (" << msg_bytes << " bytes)" << std::endl;

    /*
     * Wait for RdKafka to decommission.
     * This is not strictly needed (with check outq_len() above), but
     * allows RdKafka to clean up all its resources before the application
     * exits so that memory profilers such as valgrind wont complain about
     * memory leaks.
     */

    RdKafka::wait_destroyed(5);

    return 0;
}