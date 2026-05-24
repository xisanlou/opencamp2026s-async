use reqwest::Client;
use tokio::fs::{File, create_dir_all};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use url::Url;
use std::thread::sleep;

use std::time::Instant;
use anyhow::{Context, Error, Result};
use std::sync::Arc;
use tokio::sync::Mutex;

use sysinfo::System;
use std::time::Duration;

// static  USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
static USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) Gecko/20100101 Firefox/148.0";

async fn crawl(url: Url, client: Client, file_name: &str) -> Result<u128> {
    let now = Instant::now();

    let response = client
        .get(url.clone())
        .header("User-Agent", USER_AGENT)
        .send()
        .await
        .context(format!("Failed to fetch URL: {}", &url))?;

    if !response.status().is_success() {
        //println!("下载网站失败：{}  状态：{}", &url, response.status());
        return Err(Error::msg("Failed to fetch website"));
    }

    // Save response html to file
    let mut file = File::create(file_name).await?;
    let body = response
        .text()
        .await
        .context("Failed to read response body.")?;
    file.write_all(body.as_bytes()).await?;
    file.flush().await?;
    //println!("完成下载文件：{}", file_name);

    let mut sys = System::new_all();
    sys.refresh_all();
    let pid = sysinfo::Pid::from_u32(std::process::id());
    let mut pr_pid = true;
    let mut mem_use: u64 = 0;
    while pr_pid {
        if let Some(process) = sys.process(pid) {
            //println!("发送请求时进程内存使用： {}MB", process.memory()/1024/1024);
            mem_use = process.memory()/1024/1024;
            //println!("发送请求时进程虚拟内存使用： {}MB", process.virtual_memory()/1024/1024);
            pr_pid = false;
            if pr_pid == true {
                let one_sec = Duration::from_secs(1);
                println!("{} 未得到内存使用，睡眠1秒！！", file_name);
                sleep(one_sec);
            }
        }
    }

    let delay_millis = now.elapsed().as_millis();
    println!("filename = {} delay_millis = {}  mem_use = {}", file_name, delay_millis, mem_use);
    Ok(now.elapsed().as_millis())
}

async fn read_two_colum_file(
    filename: &str, 
    websites: &mut Vec<(String, Url)>,
) -> Result<()> {
    let file = File::open(filename).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    //let mut result = Vec::new();

    while let Some(line_result) = lines.next_line().await? {
        let line = line_result.trim();
        if line.is_empty() {continue;}
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 {
            let url = Url::parse(parts[1])?;
            let tuple = (parts[0].to_string(), url);
            websites.push(tuple);
        } else {
            println!("警告: 文件 {} 不是两列！", filename);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() ->  Result<(), Box<dyn std::error::Error>> {
    // 获取当前进程Pid
    let mut sys = System::new_all();
    sys.refresh_all();
    let pid = sysinfo::Pid::from_u32(std::process::id());

    let mut pr_pid = true;
    while pr_pid {
        if let Some(process) = sys.process(pid) {
            println!("开始时进程内存使用： {}MB", process.memory()/1024/1024);
            println!("开始时进程虚拟内存使用： {}MB", process.virtual_memory()/1024/1024);
            pr_pid = false;
            if pr_pid == true {
                let one_sec = Duration::from_secs(1);
                sleep(one_sec);
            }
        }
    }
    let result_save_dir = "websiteshtml/";
    let _ = create_dir_all(result_save_dir).await;

    // read websites file
    let mut websites = Vec::new();
    match read_two_colum_file("university-websites.txt", &mut websites).await {
        Ok(_) => {
            println!("读取网站列表成功！");
        },
        Err(e) => {
            println!("读取网站列表失败！ Error: {}", e);
        }
    }

    if websites.len() == 0 {
        println!("警告：网站列表为空！");
    }

    //let user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
    //println!("调试： 构建Client开始。");
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()?;
    //println!("调试：构建Client成功。");
    let count = Arc::new(Mutex::new(0u64));
    let delay_times = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];
    let start_time = Instant::now();
    for (name, url) in websites {
        let file_name = result_save_dir.to_string() + &name + ".html";
        let client_clone = client.clone();
        let count_clone = Arc::clone(&count);
        let delay_times_clone = Arc::clone(&delay_times);
        let handle = tokio::spawn(async move{
            match crawl(url, client_clone, &file_name).await {
                Ok(delay_time) => {
                    //println!("调试：爬取成功一次！");
                    let mut c = count_clone.lock().await;
                    *c += 1;
                    let mut d = delay_times_clone.lock().await;
                    d.push(delay_time);
                },
                Err(_) => {
                    //println!("爬取网站失败一次: {}", e);
                    
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    if let Some(process) = sys.process(pid) {
        println!("结束时进程内存使用： {}MB", process.memory()/1024/1024);
        println!("结束时进程虚拟内存使用： {}MB", process.virtual_memory()/1024/1024);
    }

    let elasped = start_time.elapsed().as_secs_f64();
    let final_count = *(count.lock().await);
    if final_count == 0 {
        println!("爬取网站数目为零！")
    } else {
        //println!("调试：final_count={}", final_count);
        let qps = final_count as f64 / elasped;
        let mut delay_times = delay_times.lock().await;
        delay_times.sort();
        let average_delay_secs = delay_times.iter().sum::<u128>() as f64 / (1000.0 * delay_times.len() as f64);

        println!("Total Request: {}", final_count);
        println!("Time Elasped: {:.5}s", elasped);
        println!("Throughput(QPS): {:.5}", qps);
        println!("Average Delay Seconds: {:.5}", average_delay_secs);
        //println!("Delay Milliseconds List: {:#?}", delay_times);
    }
    Ok(())
}
