
use std::fs::{File};
use std::io::{self, Write, BufRead, BufReader};
use std::process::Command;
use std::thread;
use std::time::Instant;
use anyhow::{Result};
use std::sync::{Arc, Mutex};





fn read_two_colum_file(
    filename: &str, 
    websites: &mut Vec<(String, String)>,
) -> Result<()> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    //let mut result = Vec::new();

    while let Some(Ok(line_result)) = lines.next() {
        let line = line_result.trim();
        if line.is_empty() {continue;}
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 {
            let tuple = (parts[0].to_string(), parts[1].to_string());
            websites.push(tuple);
        } else {
            println!("警告: 文件 {} 不是两列！", filename);
        }
    }
    Ok(())
}


fn main() ->  Result<(), Box<dyn std::error::Error>> {
    let req_command = "./curl_one";

    

    // read websites file
    let mut websites = Vec::new();
    match read_two_colum_file("university-websites.txt", &mut websites) {
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

    
    let count = Arc::new(Mutex::new(0u64));
    let mut handles = vec![];
    let start_time = Instant::now();
    for (name, url) in websites {
        let count_clone = Arc::clone(&count);
        let handle = thread::spawn(move || {
            let output = Command::new(req_command)
                    .arg("--name")
                    .arg(&name)
                    .arg("--url")
                    .arg(&url)
                    .output()
                    .expect("Failed to execute curl");
            if output.status.success() {
                let mut c = count_clone.lock().unwrap();
                *c += 1;
                io::stdout().write_all(&output.stdout).unwrap();
            }
            
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }

    

    let elasped = start_time.elapsed().as_secs_f64();
    let final_count = *(count.lock().unwrap());
    if final_count == 0 {
        println!("爬取网站数目为零！")
    } else {
        //println!("调试：final_count={}", final_count);
        let qps = final_count as f64 / elasped;
        println!("Total Request: {}", final_count);
        println!("Time Elasped: {:.5}s", elasped);
        println!("Throughput(QPS): {:.5}", qps);
        //println!("Delay Milliseconds List: {:#?}", delay_times);
    }
    Ok(())
}
