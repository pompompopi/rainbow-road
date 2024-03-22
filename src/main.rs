use std::sync::Arc;

use anyhow::{bail, Result};
use async_compression::tokio::write::BrotliEncoder;
use clap::Parser;
use regex::Regex;
use reqwest::{Client, Url};
use scraper::{Html, Selector};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    join, spawn,
    sync::broadcast::{self, error::RecvError, Sender},
};
use tokio_tar::{Builder, Header};
use tracing::info;
use tracing_subscriber::fmt;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    /// The initial chapter to start the scraping from. Specify multiple (-i url -i url) to scrape multiple fictions.
    initial_chapter: Vec<String>,
    #[arg(
        short,
        long,
        default_value = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/122.0.2365.106"
    )]
    /// The user agent to use when making requests to RoyalRoad
    user_agent: String,
}

async fn parse_chapters(
    paragraph_selector: &Selector,
    chapter_button_selector: &Selector,
    client: &Client,
    url: Url,
    archive: Sender<(String, Vec<u8>)>,
) -> Result<()> {
    let chapter_name = match url.path_segments().and_then(|segments| segments.last()) {
        Some(chapter_name) => chapter_name.to_owned(),
        None => bail!("chapter does not have name"),
    };

    let document = Html::parse_document(&client.get(url.clone()).send().await?.text().await?);
    let chapter_contents = document
        .select(paragraph_selector)
        .map(|ele| {
            ele.text()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join("")
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    info!("parsed chapter {chapter_name}");

    let next_button_link = document
        .select(chapter_button_selector)
        .find(|button| {
            let original_text = button.text().collect::<String>().to_lowercase();
            let cleaned_text = original_text.trim();

            if cleaned_text == "next chapter" {
                true
            } else {
                false
            }
        })
        .and_then(|button| button.attr("href"));

    if let Some(next_chapter) = next_button_link {
        let sender_clone = archive.clone();
        spawn(async move {
            sender_clone
                .send((chapter_name + ".txt", chapter_contents.into_bytes()))
                .unwrap();
        });
        Box::pin(parse_chapters(
            paragraph_selector,
            chapter_button_selector,
            client,
            url.join(next_chapter)?,
            archive.clone(),
        ))
        .await
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt().init();
    let args = Args::parse();
    let client = Client::builder().user_agent(args.user_agent).build()?;
    let paragraph_selector =
        Arc::new(Selector::parse("div.chapter-inner.chapter-content > p").unwrap());
    let chapter_button_selector = Arc::new(Selector::parse("a.btn.btn-primary.col-xs-12").unwrap());

    for initial_chapter in args.initial_chapter {
        let (sender, mut receiver) = broadcast::channel(10000);
        let base_name = match Regex::new(r"/fiction/\d+/([\w-]+)")?
            .captures(&initial_chapter)
            .and_then(|captures| captures.get(1))
            .map(|group| group.as_str())
        {
            Some(base_name) => base_name.to_owned(),
            None => bail!("could not guess fiction name from first chapter url"),
        };

        let url = Url::parse(&initial_chapter)?;

        let (parse_result, death_result) = join!(
            parse_chapters(
                &paragraph_selector,
                &chapter_button_selector,
                &client,
                url,
                sender
            ),
            async move {
                let mut file = File::create(format!("{base_name}.tar.br")).await?;
                let mut buf_writer = BufWriter::new(&mut file);
                let mut archive_builder =
                    Builder::new_non_terminated(BrotliEncoder::new(&mut buf_writer));

                loop {
                    let (name, bytes): (String, Vec<u8>) = match receiver.recv().await {
                        Ok(tuple) => tuple,
                        Err(e) => match e {
                            RecvError::Lagged(_) => continue,
                            _ => break,
                        },
                    };

                    let mut header = Header::new_gnu();
                    header.set_size(bytes.len() as u64);
                    header.set_mode(0o777);
                    archive_builder
                        .append_data(&mut header, &name, bytes.as_slice())
                        .await?;
                    info!("added chapter {name} to archive")
                }

                archive_builder.into_inner().await?.shutdown().await?;
                buf_writer.flush().await?;
                file.flush().await?;
                file.sync_all().await?;
                Ok::<_, anyhow::Error>(())
            }
        );

        parse_result?;
        death_result?;
    }
    Ok(())
}
