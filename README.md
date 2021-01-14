# photon-dance-delay-queue

redis-based delay queue is designed to provide message queue with delay-job-distribution.

![](doc/arch.png) 

## Get Started

### Prerequisites

```text
1. debian/ubuntu linux/x86-64 release
2. go1.12.7 linux/amd64 or higher
```

### Installation

#### Clone

* Clone this repo to your local machine using https://github.com/amazingchow/photon-dance-delay-queue.git.

#### Setup

```shell
make build

./photon-dance-delay-queue --conf=conf/delay_queue.json --verbose=true
```

## Reference

* [有赞延迟队列设计](https://tech.youzan.com/queuing_delay/)

## Contributing

### Step 1

* 🍴 Fork this repo!

### Step 2

* 🔨 HACK AWAY!

### Step 3

* 🔃 Create a new PR using https://github.com/amazingchow/photon-dance-delay-queue/compare!

## Support

* Reach out to me at <jianzhou42@163.com>.

## License

* This project is licensed under the MIT License - see the **[MIT license](http://opensource.org/licenses/mit-license.php)** for details.