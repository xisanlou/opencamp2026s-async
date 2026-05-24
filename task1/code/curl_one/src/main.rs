use reqwest::blocking::Client;
use std::fs::{File, create_dir_all};
use std::io::Write;
use url::Url;
use std::time::Instant;
use anyhow::{Context, Error, Result};
use sysinfo::{System, Pid, get_current_pid};
use clap::Parser;

// static  USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
static USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) Gecko/20100101 Firefox/148.0";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 名称
    #[arg(short, long)]
    name: String,
    /// Url
    #[arg(short, long)]
    url: String,
    /// 网页存储目录
    #[arg(short, long, default_value_t = String::from("websiteshtml/"))]
    path: String,
}

fn calculate_tree_mem(sys: &System, pid: Pid) -> u64 {
    let mut total_mem = 0;
    if let Some(process) = sys.process(pid) {
        total_mem += process.memory();
    } else {
        return 0;
    }

    for (child_pid, process) in sys.processes() {
        if process.parent() == Some(pid) {
            total_mem += calculate_tree_mem(sys, *child_pid);
        }
    }
    total_mem
}

fn main() ->  Result<(), Box<dyn std::error::Error>> {
    let now = Instant::now();

    // 解析命令行参数
    let args = Args::parse();
    let name = args.name;
    let url_name =args.url;
    let path = args.path;

    // 创建网页存储目录
    let _ = create_dir_all(&path);

    // 网页文件名
    let file_name = path + &name + ".html";

    
    // 创建一个client
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .build()?;

    // 解析Url
    let url = Url::parse(&url_name)?;
    
    //  访问url获取首页
    let response = client
        .get(url.clone())
        .header("User-Agent", USER_AGENT)
        .send()
        .context(format!("Failed to fetch URL: {}", &url))?;

    if !response.status().is_success() {
        //println!("下载网站失败：{}  状态：{}", &url, response.status());
        return Err(Error::msg("Failed to fetch website").into_boxed_dyn_error());
    }

    // Save response html to file
    let mut file = File::create(file_name)?;
    let body = response.text()?;
    file.write_all(body.as_bytes())?;
    file.flush()?;

    // 统计父进程组的内存使用
    let mut sys = System::new_all();
    sys.refresh_all();

    let current_pid = get_current_pid()?;
    let parent_pid = sys.process(current_pid).unwrap().parent().unwrap();
    let used_mem = calculate_tree_mem(&sys, parent_pid) / 1014 /1024;

   
    //计算延迟时间
    let delay_millis = now.elapsed().as_millis();
    println!("内存使用MB = {}  延迟毫秒 = {}", used_mem, delay_millis);
    Ok(())
}
