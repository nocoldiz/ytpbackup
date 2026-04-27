#!/usr/bin/env python3
"""
YTP Backup — YouTube Downloader (Interactive)
=============================================
Scans YTP nostrane / YTP fai da te forum pages for YouTube links,
fetches video description + channel info from YouTube, then downloads.

Requirements:
    pip install yt-dlp beautifulsoup4 lxml
"""

import os
import re
import sys
import json
import time
import glob
import shutil
import subprocess
import argparse
import datetime
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup

# ── Sections to scan ──────────────────────────────────────────────────────────

SCAN_SECTIONS = ["YTP nostrane", "YTP fai da te", "YTPMV dimportazione", "YTP da internet"]

# ── Disallowed channels (never scraped; removed from index if present) ────────

DISALLOWED_CHANNELS = ["Yotobi", "Croix89","animorphy","Beeeerdman","foreverKirby","TorNis Entertainment","PineappleDisciple","DarkestIntellect" "QDSS","Skillet","PeendulumLive","twkmedia","Valerio Salsero""UCn-K7GIs62ENvdQe6ZZk9-w","FunAvenue","The Chalkeaters","Fabio Mariano","Pogo","Computron"]

# ── Allowed channels (always scraped with keyword filter) ─────────────────────
    #"https://www.youtube.com/@NocoldizTV",

ALLOWED_CHANNELS = [

]
ITALIAN_CHANNELS = [
    "https://www.youtube.com/@mrpoldoakbar2849",
    "https://www.youtube.com/@TottiBest92",
    "https://www.youtube.com/@despotaaa",
    "https://www.youtube.com/@bassman85x",
    "https://www.youtube.com/@ZioTok83",
    "https://www.youtube.com/@tracFelix96trac",
    "https://www.youtube.com/@voodoochildytp",
    "https://www.youtube.com/@blazor67",
    "https://www.youtube.com/@EliaForce1984ita",
    "https://www.youtube.com/@Mario6493",
    "https://www.youtube.com/channel/UCZV0SS8CSHVWN8X6tKf4ANg",
    "https://www.youtube.com/@Pennaz",
    "https://www.youtube.com/@DarkCoffe64",
    "https://www.youtube.com/@revergo",
    "https://www.youtube.com/@MastercastPresident",
    "https://www.youtube.com/user/StewBarzTube",
    "https://www.youtube.com/user/PierluPoops",
    "https://www.youtube.com/user/ChristianIce",
    "https://www.youtube.com/user/ZioMeso",
    "https://www.youtube.com/user/julaoa",
    "https://www.youtube.com/user/JakkoMatto",
    "https://www.youtube.com/@Clodd",
    "https://www.youtube.com/c/RTpoop",
    "https://www.youtube.com/@DavvoYTP",
    "https://www.youtube.com/user/125Replay",
    "https://www.youtube.com/@AlePoops",
    "https://www.youtube.com/@frederickfrankenstein5671",
    "https://www.youtube.com/@caprastrabica2182",
    "https://www.youtube.com/c/ilCirox",
    "https://www.youtube.com/c/AssoDiDenari",
    "https://www.youtube.com/c/SassoStrappato",
    "https://www.youtube.com/c/MPYTP",
    "https://www.youtube.com/@Alel_",
    "https://www.youtube.com/@IdiotCamel",
    "https://youtube.com/@ilfilincazzatoytp",
    "https://www.youtube.com/@CerealKillzYTP",
    "https://youtube.com/@ItalianHousePoop",
    "https://youtube.com/@Tj8w",
    "https://youtube.com/@p00pbuster",
    "https://www.youtube.com/channel/UCsZ-gR0qOy4fCNG2G8Swuhw",
    "https://www.youtube.com/@rohan_ytp/featured",
    "https://www.youtube.com/@vry-dab",
    "https://youtube.com/@AssoDiDenari",
    "https://youtube.com/@RanaBastarda",
    "https://www.youtube.com/channel/UC7Y-kAwZdFELamgM31alpPg",
    "https://youtube.com/@TheGabryOfficial",
    "https://www.youtube.com/channel/UCULCU79tkDsZYaVCBF0Lmhw",
    "https://youtube.com/@cristianpoops2.0",
    "https://youtube.com/truocolo",
    "https://youtube.com/truocolo",
    "https://www.youtube.com/c/allafacciatua_xd",
    "https://www.youtube.com/@Loller97",
    "https://youtube.com/@xeduss.",
    "https://www.youtube.com/@francoytp",
    "https://youtube.com/@antchannel",
    "https://www.youtube.com/@Ge%C9%9Bg",
    "https://youtube.com/@aureliogame99",
    "https://youtube.com/@giusepoop",
    "https://www.youtube.com/@100GameReaperYTP",
    "https://www.youtube.com/@125Replay",
    "https://www.youtube.com/@2ndCenturyFox",
    "https://www.youtube.com/@Ace98100",
    "https://www.youtube.com/@Achille12345",
    "https://www.youtube.com/@AgelessObsession",
    "https://www.youtube.com/@Aladauqs",
    "https://www.youtube.com/@AlbyTree",
    "https://www.youtube.com/@AlePoops",
    "https://www.youtube.com/@Alel_",
    "https://www.youtube.com/@AlessandroRosmo",
    "https://www.youtube.com/@AlexanderFazio",
    "https://www.youtube.com/@Andrea97YTPs",
    "https://www.youtube.com/@AntonioJuliano88",
    "https://www.youtube.com/@Aros24",
    "https://www.youtube.com/@AssoDiDenari",
    "https://www.youtube.com/@B5P666c",
    "https://www.youtube.com/@BDSbowling",
    "https://www.youtube.com/@BarnabasB",
    "https://www.youtube.com/@Barsay",
    "https://www.youtube.com/@Blazor67",
    "https://www.youtube.com/@BleachGuitar",
    "https://www.youtube.com/@BlimfYTP",
    "https://www.youtube.com/@Bobomb83",
    "https://www.youtube.com/@Boltryke",
    "https://www.youtube.com/@Boogidyboo",
    "https://www.youtube.com/@BowenKainZ",
    "https://www.youtube.com/@Breakass123",
    "https://www.youtube.com/@ButtonsTheDragon",
    "https://www.youtube.com/@CaptnFalcon",
    "https://www.youtube.com/@Captpan6",
    "https://www.youtube.com/@CerealKillzYTP",
    "https://www.youtube.com/@Cheez764",
    "https://www.youtube.com/@ChristianIce",
    "https://www.youtube.com/@Clodd97",
    "https://www.youtube.com/@ClubSound18",
    "https://www.youtube.com/@CommanderMorshu",
    "https://www.youtube.com/@CorruptionSound",
    "https://www.youtube.com/@CraaazyCat13",
    "https://www.youtube.com/@CrazyPooper",
    "https://www.youtube.com/@CuddlyDream",
    "https://www.youtube.com/@DWL1993",
    "https://www.youtube.com/@DanThoRiu",
    "https://www.youtube.com/@DarkCoffe64",
    "https://www.youtube.com/@DarkestIntellect",
    "https://www.youtube.com/@Davaia",
    "https://www.youtube.com/@DavideToroh",
    "https://www.youtube.com/@DeadpoolDamian",
    "https://www.youtube.com/@DeltaHF89",
    "https://www.youtube.com/@DemyGod",
    "https://www.youtube.com/@DiagloTheBriosser",
    "https://www.youtube.com/@DinWar",
    "https://www.youtube.com/@DioNero94",
    "https://www.youtube.com/@DjpoopOfficial",
    "https://www.youtube.com/@DoctorChub",
    "https://www.youtube.com/@Doskey",
    "https://www.youtube.com/@DownOnTheBrazos",
    "https://www.youtube.com/@DrRustico",
    "https://www.youtube.com/@DubskiDude",
    "https://www.youtube.com/@Dumno7",
    "https://www.youtube.com/@ElementNx",
    "https://www.youtube.com/@EliaForce1984ita",
    "https://www.youtube.com/@EpicLPer",
    "https://www.youtube.com/@EvilYorkiz",
    "https://www.youtube.com/@FCChannelTubePoop",
    "https://www.youtube.com/@FancazzistiAnonimi",
    "https://www.youtube.com/@Federigoo112",
    "https://www.youtube.com/@Flippy952",
    "https://www.youtube.com/@Forza97",
    "https://www.youtube.com/@Franchiowtf",
    "https://www.youtube.com/@Fraws87",
    "https://www.youtube.com/@FriendlyWarlord",
    "https://www.youtube.com/@Gamemasternumberone",
    "https://www.youtube.com/@Garrysmodita",
    "https://www.youtube.com/@Geibuchan",
    "https://www.youtube.com/@Gertilish",
    "https://www.youtube.com/@GliUseless",
    "https://www.youtube.com/@GoldLucario97",
    "https://www.youtube.com/@Gordon91",
    "https://www.youtube.com/@GreatBritishTurd",
    "https://www.youtube.com/@HIKIKOMORI000",
    "https://www.youtube.com/@HalDanGhor",
    "https://www.youtube.com/@HaloJaxed",
    "https://www.youtube.com/@IlReDellePoop",
    "https://www.youtube.com/@IlTrioPoop",
    "https://www.youtube.com/@IndieGameMusicHD",
    "https://www.youtube.com/@Informatopi",
    "https://www.youtube.com/@Iperciuk",
    "https://www.youtube.com/@ItalianHousePoop",
    "https://www.youtube.com/@IzanagiYTP",
    "https://www.youtube.com/@JJokerDude",
    "https://www.youtube.com/@JacksonJunior101",
    "https://www.youtube.com/@Jakabu128",
    "https://www.youtube.com/@JakkoMatto",
    "https://www.youtube.com/@JeffLindblom",
    "https://www.youtube.com/@Jep93XxXNA",
    "https://www.youtube.com/@JoltJolteon",
    "https://www.youtube.com/@JornalismoQuebrado",
    "https://www.youtube.com/@KRDBrando",
    "https://www.youtube.com/@KefkaFTW",
    "https://www.youtube.com/@KojiKabutoITA",
    "https://www.youtube.com/@Kuhneghetz",
    "https://www.youtube.com/@KyoSMind",
    "https://www.youtube.com/@LaGuardiaReale",
    "https://www.youtube.com/@Laizorb",
    "https://www.youtube.com/@Laretski",
    "https://www.youtube.com/@LeNoirLive",
    "https://www.youtube.com/@Leemone",
    "https://www.youtube.com/@LegendarySage",
    "https://www.youtube.com/@LightningToast3",
    "https://www.youtube.com/@Loller97",
    "https://www.youtube.com/@LukTrek",
    "https://www.youtube.com/@Lumpytoast",
    "https://www.youtube.com/@M0rtanius",
    "https://www.youtube.com/@METAL666MILITIA",
    "https://www.youtube.com/@MPCozmo",
    "https://www.youtube.com/@MPYTP",
    "https://www.youtube.com/@ManakoBs",
    "https://www.youtube.com/@MaraudersClub",
    "https://www.youtube.com/@Mario6493",
    "https://www.youtube.com/@MasterAl",
    "https://www.youtube.com/@MastercastPresident",
    "https://www.youtube.com/@McMaNGOS",
    "https://www.youtube.com/@MclovinKillerMX",
    "https://www.youtube.com/@MeStarStudios",
    "https://www.youtube.com/@MectaPoopITA",
    "https://www.youtube.com/@MediaMunkee",
    "https://www.youtube.com/@MetaAndre11",
    "https://www.youtube.com/@MewMewJoanna",
    "https://www.youtube.com/@Michela_life_oac",
    "https://www.youtube.com/@MilkshakeManCP",
    "https://www.youtube.com/@MisterS0sa",
    "https://www.youtube.com/@Moto200Alt",
    "https://www.youtube.com/@MrApocalisse",
    "https://www.youtube.com/@MrDuePenny",
    "https://www.youtube.com/@MrFp96",
    "https://www.youtube.com/@MrLucario2",
    "https://www.youtube.com/@MrRoboto113",
    "https://www.youtube.com/@MrTennek",
    "https://www.youtube.com/@MrVernechannel",
    "https://www.youtube.com/@MuzicFreakNumberOne",
    "https://www.youtube.com/@MycroProcessor",
    "https://www.youtube.com/@NefosG",
    "https://www.youtube.com/@NikiPoop",
    "https://www.youtube.com/@NiklasPooper",
    "https://www.youtube.com/@NocoldizTV",
    "https://www.youtube.com/@Nprp",
    "https://www.youtube.com/@Ottodorp",
    "https://www.youtube.com/@OutoMaisteri",
    "https://www.youtube.com/@OznerolDeAngelis",
    "https://www.youtube.com/@PaandamanYo",
    "https://www.youtube.com/@PatrickLify",
    "https://www.youtube.com/@PeppeJep93",
    "https://www.youtube.com/@PhantomDusclops92",
    "https://www.youtube.com/@Phantomat14",
    "https://www.youtube.com/@Pictocheat",
    "https://www.youtube.com/@PigHunter4",
    "https://www.youtube.com/@PizzaPony",
    "https://www.youtube.com/@PooPTuBeNooB",
    "https://www.youtube.com/@PoopMasta88",
    "https://www.youtube.com/@PoopPinchesBack",
    "https://www.youtube.com/@PoopSlammer",
    "https://www.youtube.com/@PresidentOfJelybeans",
    "https://www.youtube.com/@PyrotheBest",
    "https://www.youtube.com/@Pyrstoyska",
    "https://www.youtube.com/@Qualcunaltro1",
    "https://www.youtube.com/@QueITizio",
    "https://www.youtube.com/@QuibbyJibby",
    "https://www.youtube.com/@Raf_Tama",
    "https://www.youtube.com/@RanaBastarda",
    "https://www.youtube.com/@Ratmus1",
    "https://www.youtube.com/@Remyrue",
    "https://www.youtube.com/@RenardQueenston",
    "https://www.youtube.com/@RocchioSciamenna",
    "https://www.youtube.com/@SLBysusparidas",
    "https://www.youtube.com/@SecretaryEle",
    "https://www.youtube.com/@SelceTeamProductions",
    "https://www.youtube.com/@SermetraScPA",
    "https://www.youtube.com/@ShadowtheKnuckles",
    "https://www.youtube.com/@ShinRaNewlyEmployed",
    "https://www.youtube.com/@ShroomheadOne",
    "https://www.youtube.com/@SimixF1",
    "https://www.youtube.com/@Sir_Daniel",
    "https://www.youtube.com/@Skullgirl9",
    "https://www.youtube.com/@Spaceoffz",
    "https://www.youtube.com/@Spazza17",
    "https://www.youtube.com/@Spritanium",
    "https://www.youtube.com/@StackBrains",
    "https://www.youtube.com/@StarRodMan",
    "https://www.youtube.com/@StewBarzTube",
    "https://www.youtube.com/@SuperYoshi",
    "https://www.youtube.com/@SuperdarkIuigi4ever",
    "https://www.youtube.com/@Surplusx21",
    "https://www.youtube.com/@SvenFletcher",
    "https://www.youtube.com/@SwishFilmsinc",
    "https://www.youtube.com/@THEpillo234",
    "https://www.youtube.com/@TabooVudu",
    "https://www.youtube.com/@Tachin1994",
    "https://www.youtube.com/@TarantoEvangelica",
    "https://www.youtube.com/@Teletubbiepoop",
    "https://www.youtube.com/@TeruChanLand",
    "https://www.youtube.com/@TheDarkRises",
    "https://www.youtube.com/@TheExtremeTE",
    "https://www.youtube.com/@TheFelixxxmaster",
    "https://www.youtube.com/@TheFilippoop",
    "https://www.youtube.com/@TheGamerOfAllGamers",
    "https://www.youtube.com/@TheKingOFKings69611",
    "https://www.youtube.com/@TheKingofSilverFoxes",
    "https://www.youtube.com/@TheKooperPooper",
    "https://www.youtube.com/@TheLaxOne",
    "https://www.youtube.com/@TheNightwisher88",
    "https://www.youtube.com/@TheNoelagghijesu",
    "https://www.youtube.com/@TheSfronzMovies",
    "https://www.youtube.com/@TheSpeedKing96",
    "https://www.youtube.com/@TheTano97",
    "https://www.youtube.com/@TheTehniga",
    "https://www.youtube.com/@ThemuseshoneY",
    "https://www.youtube.com/@Themysteriouspirate",
    "https://www.youtube.com/@TimoteiLSD",
    "https://www.youtube.com/@Tj8w",
    "https://www.youtube.com/@ToopofthePoop",
    "https://www.youtube.com/@TottiBest92",
    "https://www.youtube.com/@TranceDJnewbie",
    "https://www.youtube.com/@Trapinch12",
    "https://www.youtube.com/@TukariSilver",
    "https://www.youtube.com/@USBduck",
    "https://www.youtube.com/@UberNooberPooper",
    "https://www.youtube.com/@Ultimooooooooo",
    "https://www.youtube.com/@UnNicknameOriginale",
    "https://www.youtube.com/@UomoBlooper",
    "https://www.youtube.com/@UtenteMacSenzaMac",
    "https://www.youtube.com/@VRY-DAB",
    "https://www.youtube.com/@Vacantification",
    "https://www.youtube.com/@ValeGadogni",
    "https://www.youtube.com/@Veksler96",
    "https://www.youtube.com/@Victinho6D",
    "https://www.youtube.com/@Vid3able",
    "https://www.youtube.com/@Voiaganto",
    "https://www.youtube.com/@VoodooChildYTP",
    "https://www.youtube.com/@Vorhias",
    "https://www.youtube.com/@Vurrix",
    "https://www.youtube.com/@WLB91",
    "https://www.youtube.com/@Whopperized",
    "https://www.youtube.com/@Whyimnotfat",
    "https://www.youtube.com/@XXBlackLyon92XX",
    "https://www.youtube.com/@XblowLyourBMind",
    "https://www.youtube.com/@Yiulias",
    "https://www.youtube.com/@Youtubors",
    "https://www.youtube.com/@YovanniYoni",
    "https://www.youtube.com/@Zaburac",
    "https://www.youtube.com/@Zeb89",
    "https://www.youtube.com/@Zeroxxz11",
    "https://www.youtube.com/@ZioMeso",
    "https://www.youtube.com/@ZioTok83",
    "https://www.youtube.com/@age3rcm530",
    "https://www.youtube.com/@alberisecchi7647",
    "https://www.youtube.com/@alfonsoamendola3262",
    "https://www.youtube.com/@allafacciatua_xd",
    "https://www.youtube.com/@anonymi",
    "https://www.youtube.com/@anonymide",
    "https://www.youtube.com/@antoniocovatta",
    "https://www.youtube.com/@arghivebeenshot",
    "https://www.youtube.com/@ashhousewares445",
    "https://www.youtube.com/@avojaifnot",
    "https://www.youtube.com/@axelrod777",
    "https://www.youtube.com/@bassman85x",
    "https://www.youtube.com/@blazor69secondocanaledibla51",
    "https://www.youtube.com/@boari994",
    "https://www.youtube.com/@bosch002",
    "https://www.youtube.com/@briotera",
    "https://www.youtube.com/@cABit94",
    "https://www.youtube.com/@canesecco",
    "https://www.youtube.com/@caprastrabica2182",
    "https://www.youtube.com/@celsowm",
    "https://www.youtube.com/@cinemaverita",
    "https://www.youtube.com/@concadoreno",
    "https://www.youtube.com/@crapagent",
    "https://www.youtube.com/@crbenesch",
    "https://www.youtube.com/@cupoficewater1",
    "https://www.youtube.com/@cyphermur9t",
    "https://www.youtube.com/@darkturn",
    "https://www.youtube.com/@demenzialproject1942",
    "https://www.youtube.com/@derdingobaron",
    "https://www.youtube.com/@despotaaa",
    "https://www.youtube.com/@djgiorgio97",
    "https://www.youtube.com/@dreameroflove",
    "https://www.youtube.com/@dreamland94",
    "https://www.youtube.com/@eduardorpg64",
    "https://www.youtube.com/@electricthecheese",
    "https://www.youtube.com/@elibatsni",
    "https://www.youtube.com/@ensisarts",
    "https://www.youtube.com/@eugimosco",
    "https://www.youtube.com/@fabulousfreebirds",
    "https://www.youtube.com/@fagiolone83",
    "https://www.youtube.com/@frederickfrankenstein5671",
    "https://www.youtube.com/@gamepopper101",
    "https://www.youtube.com/@ganonvslink1000",
    "https://www.youtube.com/@gb7zone7",
    "https://www.youtube.com/@geogeobananarap",
    "https://www.youtube.com/@gianluca104poops",
    "https://www.youtube.com/@giganticproblem666",
    "https://www.youtube.com/@gionniovarb",
    "https://www.youtube.com/@giromirgork3705",
    "https://www.youtube.com/@guilhox",
    "https://www.youtube.com/@guysafari",
    "https://www.youtube.com/@idiotcamel",
    "https://www.youtube.com/@ilCirox",
    "https://www.youtube.com/@ilpoveroComunistah",
    "https://www.youtube.com/@imaperson180",
    "https://www.youtube.com/@inspecterclouseau",
    "https://www.youtube.com/@ipoopanti5961",
    "https://www.youtube.com/@irover",
    "https://www.youtube.com/@jeroenpompoen",
    "https://www.youtube.com/@johncrow6541",
    "https://www.youtube.com/@julaoa",
    "https://www.youtube.com/@kagemaru026",
    "https://www.youtube.com/@kerby",
    "https://www.youtube.com/@kevinastro",
    "https://www.youtube.com/@keyserzozzo5388",
    "https://www.youtube.com/@kifflom6910",
    "https://www.youtube.com/@kikosak1",
    "https://www.youtube.com/@kitty0706",
    "https://www.youtube.com/@kwarkman85",
    "https://www.youtube.com/@l337toaster",
    "https://www.youtube.com/@lallegrochirurgo5540",
    "https://www.youtube.com/@lamegliogioventu",
    "https://www.youtube.com/@lesslunatic",
    "https://www.youtube.com/@levusbevus",
    "https://www.youtube.com/@lianfromthestars",
    "https://www.youtube.com/@lolifante",
    "https://www.youtube.com/@luciomarco",
    "https://www.youtube.com/@lullo74",
    "https://www.youtube.com/@luxexcellence2483",
    "https://www.youtube.com/@macioilmagno",
    "https://www.youtube.com/@madanonymous",
    "https://www.youtube.com/@mamaluigi02",
    "https://www.youtube.com/@manuknife93",
    "https://www.youtube.com/@manusnake_",
    "https://www.youtube.com/@marco30074",
    "https://www.youtube.com/@mariotorrone",
    "https://www.youtube.com/@massilmitico",
    "https://www.youtube.com/@mattiapagano92",
    "https://www.youtube.com/@matusalemmeballerino8020",
    "https://www.youtube.com/@mijino",
    "https://www.youtube.com/@mikycop6",
    "https://www.youtube.com/@mr.pungolo716",
    "https://www.youtube.com/@mrpoldoakbar2849",
    "https://www.youtube.com/@mylestailschannel",
    "https://www.youtube.com/@n00bobliterator",
    "https://www.youtube.com/@nickkarion",
    "https://www.youtube.com/@nobuyukinyuu",
    "https://www.youtube.com/@nocoldizTV",
    "https://www.youtube.com/@noi4crazyguys",
    "https://www.youtube.com/@nomefigo6115",
    "https://www.youtube.com/@norris3942",
    "https://www.youtube.com/@omgtsn",
    "https://www.youtube.com/@ophios",
    "https://www.youtube.com/@oscurobaronerampante",
    "https://www.youtube.com/@p00pbuster",
    "https://www.youtube.com/@parnas1us",
    "https://www.youtube.com/@pendulum",
    "https://www.youtube.com/@pierlupoops",
    "https://www.youtube.com/@piodx",
    "https://www.youtube.com/@poFETT",
    "https://www.youtube.com/@pokemonmusicmaster",
    "https://www.youtube.com/@poopemcshit9131",
    "https://www.youtube.com/@pooppappero",
    "https://www.youtube.com/@poponicspoops5044",
    "https://www.youtube.com/@potsugoro",
    "https://www.youtube.com/@rawasir",
    "https://www.youtube.com/@reddevils500a",
    "https://www.youtube.com/@revergo",
    "https://www.youtube.com/@ruach12355",
    "https://www.youtube.com/@rujoTV",
    "https://www.youtube.com/@rushnerd",
    "https://www.youtube.com/@sausism",
    "https://www.youtube.com/@shadowxworks",
    "https://www.youtube.com/@shroomhead1tennis",
    "https://www.youtube.com/@sinpecadoweb",
    "https://www.youtube.com/@skatethelife777",
    "https://www.youtube.com/@ske1988",
    "https://www.youtube.com/@smontaggiovideo",
    "https://www.youtube.com/@spartanguy00",
    "https://www.youtube.com/@spring.pooper",
    "https://www.youtube.com/@supdawg444",
    "https://www.youtube.com/@superkingytp5482",
    "https://www.youtube.com/@svegliamiii",
    "https://www.youtube.com/@tank2tank",
    "https://www.youtube.com/@theDeamonXxX",
    "https://www.youtube.com/@thecloakedinquirer",
    "https://www.youtube.com/@thecongurt",
    "https://www.youtube.com/@thegianchi",
    "https://www.youtube.com/@tomgoodmen",
    "https://www.youtube.com/@tracFelix96trac",
    "https://www.youtube.com/@tuttoratpoopytp",
    "https://www.youtube.com/@twinx1337",
    "https://www.youtube.com/@unhoots",
    "https://www.youtube.com/@unintended84",
    "https://www.youtube.com/@universalquantifier",
    "https://www.youtube.com/@vanesso100",
    "https://www.youtube.com/@vic_vacuo",
    "https://www.youtube.com/@vlxo23",
    "https://www.youtube.com/@vortaniz",
    "https://www.youtube.com/@wazgul",
    "https://www.youtube.com/@xxsweetaddiexx",
    "https://www.youtube.com/@xycechipmusic",
    "https://www.youtube.com/c/AssoDiDenari",
    "https://www.youtube.com/c/MPYTP",
    "https://www.youtube.com/c/RTpoop",
    "https://www.youtube.com/channel/UCULCU79tkDsZYaVCBF0Lmhw",
    "https://www.youtube.com/user/ChristianIce",
    "https://www.youtube.com/user/julaoa",
    "https://youtube.com/@RanaBastarda",

]
ENGLISH_CHANELS = [
    "https://www.youtube.com/@cs188",
    "https://www.youtube.com/@KroboProductions",
    "https://www.youtube.com/@EmperorLemon",
    "https://www.youtube.com/@Deepercutt",
    "https://www.youtube.com/@Hurricoaster",
    "https://www.youtube.com/@DaThings",
]

SPANISH_CHANNELS = [
    "https://www.youtube.com/@ParodiadorAnimado",
    "https://www.youtube.com/@HDLuigi",
    "https://www.youtube.com/@Catdany",
    "https://www.youtube.com/@NinterYT",
    "https://www.youtube.com/@Reloxard",
    "https://www.youtube.com/@Catdany",
    "https://www.youtube.com/@HDLuigi",
    "https://www.youtube.com/@NinterYT",
    "https://www.youtube.com/@Reloxard",
    "https://www.youtube.com/@SLBysusparidas",
    "https://www.youtube.com/@SimixF1",
    "https://www.youtube.com/@Tachin1994"
]

GERMAN_CHANNELS = [
    "https://www.youtube.com/@PetersKotstube",
    "https://www.youtube.com/@Sostrator",
    "https://www.youtube.com/@YTKFactory",
    "https://www.youtube.com/@FanboyAllianz",
    "https://www.youtube.com/@MinerMorsel",
    "https://www.youtube.com/@CorruptionSound",
    "https://www.youtube.com/@PetersKotstube",
    "https://www.youtube.com/@ShroomheadOne",
    "https://www.youtube.com/@Sostrator",
    "https://www.youtube.com/@YTKFactory",

]
FRENCH_CHANNELS = [
    "https://www.youtube.com/@PoopSlammer",
    "https://www.youtube.com/@USBduck",
    "https://youtube.com/@Tj8w",

]

RUSSIAN_CHANNELS = [
    "https://www.youtube.com/@Whyimnotfat",
    "https://www.youtube.com/@gfoint",

]
# NocoldizTV: scrape everything except videos whose title matches these words
NOCOLDIZ_BLACKLIST = re.compile(
    r'(?i)(gameplay|hypernet|devlog|gioco|em\.Path|em\.Brace)'
)

# ── Channel keywords ──────────────────────────────────────────────────────────

CHANNEL_KEYWORDS = re.compile(
    r'(?i)(YTP|YTPMV|Collab|Youtube\s+poop|YT\s+Poop|Poop|Speciale'
    
    # --- YTPITA (ITALIAN) ---
    r'|matteo\s+montesi|avventure|Zeb|Collegio|Bigazzi|Soccer|Ganon|Billy\s+Mays|Branduardi|Luigi|Ambrogio|Risotto|ariete|Harry\s+potter|Round|Peppa|Grylls|Tennis|Acid|Favij|Testoh|Pingu'
    r'|Dipr[eè]|Bello\s+Figo|Germano|Grillo|Gesù|Nabbo|Yotobi|Berlusconi|Muniz|Travaglio|Nemesis|Testo|Papa|Super\s+Quark|Iscritti|YTM|YTG|MLG|YTK'
    r'|Sentence\s+Mix|Ear\s?rape|G-Major|Mondo\s+emo|Pubblicità|Spot|Spongebob|Reverse|Masking|Pitch\s+Shift'
    r'|Mosconi|Benson|Brumotti|Master\s?chef|Mister\s+Lui|Pappalardo|Sgarbi|Razzi|Salvini|Renzi|Rio\s+mare|Gerry\s+Scotti|Fazio'
    r'|Kabu|Nocoldiz|Poldo|Cloroformio|Giannino|Gianni\s+Morandi|Doraemon|Me\s+cont[ro]o\s+Te'
    # --- GLOBAL & ENGLISH CLASSICS ---
    r'|Pingas|CD-i|Morshu|Mah\s+Boi|He[\s-]?Man|Sparta\s+Remix|Scad|Stutter|Patrick|Jack\s+Black|Gourmet|The\s+king|Weegee|Spadinner|Michael\s+Rosen|Viacom|Skooks|Flex\s+Tape|Phil\s+Swift|Slap\s+Chop|Hotel\s+Mario|Hank\s+Hill|King\s+Harkinian|Zelda\s+CD-i'
    # --- YTPH (SPANISH) ---
    r'|YTPH|YTPHSHORT|YTPBR|Chavo\s+del\s+8|Pelea\s+de\s+invalidos|Vete\s+a\s+la\s+Versh|Pooppa[ñn]ol'
    # --- YTP FR (FRENCH) ---
    r'|YTPFR|YTP\s+FR|Brocante|Joueur\s+du\s+Grenier|JDG|Koh\s+Lanta|Denis\s+Brogniart|David\s+Goodenough'
    # --- YTK (GERMAN / YOUTUBE KACKE) ---
    r'|YouTube\s+Kacke|Marcell\s+D\'Avis|Peter\s+Zwegat|Kinski|Löwenzahn|Peter\s+Lustig|1&1'
    # --- RYTP (RUSSIAN) ---
    r'|RYTP|РУТП|Поцык|Повар|Сашко|Гамаз|Пенек)'
)

import re

NON_YTP_KEYWORDS = re.compile(
    r'(?i)('
    # --- GAMING (SERIOUS/LONGFORM) ---
    r'Walkthrough|Playthrough|Let\'s\s+Play|Gameplay|Longplay|No\s+Commentary|Speedrun|'
    r'Boss\s+Fight|Achievement\s+Guide|Trophy\s+Guide|100%\s+Completion|Quest\s+Line|'
    r'Partita|Giocata|Commento|Reazione|Reaction\s+ita|Dal\s+vivo|Streaming\s+ora|'
    r'Migliori\s+momenti|Highlights\s+live|Torneo|Guida\s+completa|'
    
    # --- TECH, REVIEWS & SHOPPING ---
    r'Unboxing|Review|Hands-on|Benchmark|Comparison|Specs|Tech\s+News|Setup|'
    r'Hardware|Software\s+Tutorial|How\s?to\s+Install|Step\s+by\s+Step|Buying\s+Guide|'
    r'Recensione|Prova|Test|Recensione\s+Onesta|Confronto|Loquendo'
    r'Cosa\s+ne\s+penso|Consigli\s+per\s+gli\s+acquisti|Scheda\s+Video|'
    
    # --- LIFESTYLE, VLOGS & TRENDS ---
    r'Vlog|Daily\s+Routine|GRWM|Get\s+Ready\s+With\s+Me|Haul|Q&A|'
    r'Ask\s+Me\s+Anything|Lifestyle|Life\s+Updates|Day\s+in\s+the\s+life|Travel\s+Diary|'
    r'La\s+mia\s+routine|Cosa\s+mangio|Vlog\s+ita|Viaggio\s+a|Domande\s+e\s+risposte|'
    r'Le\s+mie\s+opinioni|Draw\s+my\s+life\s+ita|Challenge\s+ita|'
    
    # --- OFFICIAL MEDIA, TV & NEWS ---
    r'Official\s+Music\s+Video|Lyric\s+Video|Sountrack|OST|Official\s+Trailer|Teaser\s+Trailer|'
    r'Full\s+Episode|News\s+Report|Breaking\s+News|Press\s+Conference|'
    r'Short\s+Film|Behind\s+the\s+Scenes|BTS|Making\s+of|'
    r'Puntata\s+intera|Episodio\s+completo|Film\s+completo|Versione\s+integrale|'
    r'Video\s+ufficiale|Audio\s+ufficiale|Sigla|Testo\s+canzone|Trailer\s+italiano|'
    r'Servizio|Conferenza\s+stampa|Reportage|'
    
    # --- EDUCATION & TUTORIAL ---
    r'Lecture|Webinar|Course|Seminar|Presentation|Keynote|Workshop|'
    r'Tutorial\s+for\s+beginners|Masterclass|Podcast\s+Episode|TED\s?Talk|'
    r'Tutorial\s+ita|Come\s+fare|Spiegazione|Lezione|Corso\s+di|'
    
    # --- MISC NON-POOP ---
    r'ASMR|Meditation|Workout|Fitness\s+Routine|Recipe|Cooking\s+Class|DIY\s+Crafts|'
    r'Fai\s+da\s+te'
    r')'
)

DEFAULT_SITE_DIR = "./site_mirror"
DEFAULT_VIDEO_DIR = "./videos"
DEFAULT_DOCS_DIR = "./docs"
DEFAULT_FORMAT = "bestvideo[height<=720]+bestaudio/best[height<=720]/best"

# ── YouTube URL helpers ───────────────────────────────────────────────────────

YT_PATTERNS = [
    re.compile(r'https?://(?:www\.)?youtube\.com/watch\?[^\s"\'<>]*v=[\w-]{11}[^\s"\'<>]*', re.I),
    re.compile(r'https?://youtu\.be/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube\.com/embed/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube\.com/shorts/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube-nocookie\.com/embed/([\w-]{11})[^\s"\'<>]*', re.I),
    re.compile(r'https?://(?:www\.)?youtube\.com/v/([\w-]{11})[^\s"\'<>]*', re.I),
]

YT_ID_RE = re.compile(
    r'(?:youtube\.com/(?:watch\?.*?v=|embed/|v/|shorts/)|youtu\.be/|youtube-nocookie\.com/embed/)'
    r'([\w-]{11})',
    re.I,
)

UNAVAIL_MSGS = [
    "video unavailable", "private video", "has been removed",
    "content is not available", "copyright claim",
    "account associated with this video has been terminated",
    "violates youtube's terms of service", "been removed by the uploader",
    "confirm your age", "join this channel", "members-only",
    "not available in your country", "no longer available",
]

DL_PROGRESS_RE = re.compile(
    r'\[download\]\s+([\d.]+)%\s+of\s+~?\s*([\d.]+\s*[a-zA-Z]+)(?:\s+at\s+([\d.]+\s*[a-zA-Z/]+))?'
)

ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def extract_video_id(url):
    m = YT_ID_RE.search(url)
    return m.group(1) if m else None


def canonical_yt_url(vid):
    return f"https://www.youtube.com/watch?v={vid}"


def channel_videos_url(channel_url):
    url = channel_url.rstrip("/")
    url = re.sub(r'/(videos|shorts|streams|playlists|about|community|featured)$', '', url)
    return url + "/videos"


def safe_filename(name, max_len=80):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_len]


def thread_title_from_filename(fname):
    """'71236585_Some Thread Title.html'  →  'Some Thread Title'"""
    stem = Path(fname).stem
    m = re.match(r'^\d+_(.*)', stem)
    return m.group(1) if m else stem


def bar(pct, width=28):
    filled = int(width * pct / 100)
    return "[" + "=" * filled + " " * (width - filled) + f"] {pct:5.1f}%"


def clear_line():
    cols = shutil.get_terminal_size((80, 24)).columns
    print("\r" + " " * cols + "\r", end="", flush=True)

def do_download_language(index, video_dir, yt_format, rate_limit, retry_failed, channels_list, year_limit=None):
    print(f"\n>>> Starting Language Scan for {len(channels_list)} channels...")
    
    new_entries = 0
    for chan_url in channels_list:
        base_url = chan_url.split('/featured')[0].split('/videos')[0]
        print(f"[*] Scraping channel: {base_url}",flush=True)
        
        cmd = ["yt-dlp", "--flat-playlist", "--print", "%(id)s|%(title)s|%(upload_date)s", base_url]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                if '|' not in line: continue
                v_id, v_title, v_date = line.split('|', 2)

                # Keyword Match Check
                if CHANNEL_KEYWORDS.search(v_title):
                    if v_id not in index.data and v_id not in index.excluded_ids:
                        index.add_video(
                            video_id=v_id,
                            section="Youtube",
                            source_page=f"Language Scrape ({base_url})",
                            thread_title=v_title
                        )
                        new_entries += 1
                        print(f"    [Found] Match: {v_title}", flush=True)
                        
                        # Save every 10 new entries
                        if new_entries % 10 == 0:
                            index.save()
                            print(f"    [LOG] Auto-saved index ({new_entries} new matches found so far)")
        except Exception as e:
            print(f"    [!] Error scraping {chan_url}: {e}")

    index.save()
    print(f"\n>>> Scraping complete. {new_entries} total matches added. Starting downloads...")

    # Now trigger the download for this specific set (filtered from the main index)
    do_download_youtube(index, video_dir, yt_format, rate_limit, retry_failed)

def is_disallowed_channel(channel_name):
    if not channel_name:
        return False
    name_lower = channel_name.lower()
    return any(d.lower() in name_lower for d in DISALLOWED_CHANNELS)


def is_nocoldiz_channel(ch_url, ch_name=""):
    return "nocoldiz" in (ch_url or "").lower() or "nocoldiz" in (ch_name or "").lower()

def do_download_by_section(index, video_dir, yt_format, rate_limit):
    """
    Prompts for a section from SCAN_SECTIONS and downloads pending videos 
    belonging to that section.
    """
    print("\n--- Section Download Mode ---")
    for i, section in enumerate(SCAN_SECTIONS, 1):
        print(f"{i}) {section}")
    
    try:
        # Using flush=False for input prompts is usually fine, 
        # but the main logs need it.
        choice = int(input("\nSelect section number to download: ")) - 1
        if choice < 0 or choice >= len(SCAN_SECTIONS):
            print("Invalid selection.")
            return
        selected_section = SCAN_SECTIONS[choice]
    except ValueError:
        print("Invalid input.")
        return

    # Filter pending videos that belong to this section
    to_download = []
    for v_id, info in index.data.items():
        if v_id in index.excluded_ids:
            continue
        if info.get("status") == "pending":
            if selected_section in info.get("sections", []):
                to_download.append((v_id, info))

    if not to_download:
        print(f"\n[!] No pending videos found for section: {selected_section}")
        return

    # Flush here so you see the total count before downloads start
    print(f"\n>>> Found {len(to_download)} videos to download in '{selected_section}'.", flush=True)

    download_count = 0
    for v_id, info in to_download:
        print(f"[*] [{selected_section}] Downloading: {info.get('title', 'Unknown Title')} [{v_id}]", flush=True)
        
        # 1. Resolve the channel folder (Fix starts here)
        ch_name = info.get("channel_name")
        folder_name = safe_filename(ch_name) if ch_name else "Unknown Channel"
        out_dir = os.path.join(video_dir, folder_name)
        
        # Ensure the directory exists
        os.makedirs(out_dir, exist_ok=True)
        
        # 2. Update out_tmpl to use the channel subfolder
        out_tmpl = os.path.join(out_dir, "%(title)s [%(id)s].%(ext)s")
        
        info["status"] = "downloading"
        success = False
        cmd = ["yt-dlp", "-f", yt_format, "-o", out_tmpl, "--no-playlist", "--quiet", "--no-warnings"]        
        if rate_limit:
            cmd += ["--rate-limit", rate_limit]
        cmd.append(f"https://www.youtube.com/watch?v={v_id}")

        try:
            # subprocess.run waits for yt-dlp to finish. 
            # Once it returns, the next print will fire.
            subprocess.run(cmd, check=True)
            info["status"] = "downloaded"
            success = True
            # Added flush=True: Displays "Finished" as soon as yt-dlp exits
            print(f"    [SUCCESS] Finished: {v_id}", flush=True)
        except subprocess.CalledProcessError:
            info["status"] = "failed"
            print(f"    [FAILED] Error downloading {v_id}", flush=True)

        download_count += 1
        
        # Save index every 10 entries
        if download_count % 10 == 0:
            index.save()
            print(f"    [LOG] Auto-saved progress ({download_count}/{len(to_download)})", flush=True)

    index.save() # Final save
    print(f"\n>>> Section '{selected_section}' batch complete.", flush=True)
# ── Video Index ───────────────────────────────────────────────────────────────

class VideoIndex:
    """
    {
      "VIDEO_ID": {
        "url":          "https://www.youtube.com/watch?v=...",
        "title":         str | null,
        "description":   str | null,
        "channel_name":  str | null,
        "channel_url":   str | null,
        "publish_date":  str | null,
        "view_count":    int | null,
        "like_count":    int | null,
        "tags":          list[str],
        "nickname":      str | null,    <- author of first post of source thread
        "sections":      ["YTP nostrane", ...],
        "source_pages":  ["YTP nostrane/71236585_Title.html", ...],
        "thread_titles": ["In the Madonna — Tassista Romano", ...],
        "status":        "pending" | "downloaded" | "unavailable" | "failed",
        "local_file":    str | null,
        "mirrors":       list | null,
      }
    }
    """

    def __init__(self, video_dir, docs_dir=None):
        self.video_dir = video_dir
        # Store video_index.json in docs/ for the web visualizer
        self.docs_dir = docs_dir or DEFAULT_DOCS_DIR
        self.filepath = os.path.join(self.docs_dir, "video_index.json")
        self.data = {}
        self.excluded_ids = set()
        self.load_excluded()

    def load_excluded(self):
        # excluded_videos.json is expected to be in the root directory (same as script)
        path = "excluded_videos.json"
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    excluded_data = json.load(f)
                    if isinstance(excluded_data, dict):
                        self.excluded_ids = set(excluded_data.keys())
                    elif isinstance(excluded_data, list):
                        self.excluded_ids = set(excluded_data)
            except Exception as e:
                print(f"  [!] Error loading {path}: {e}")

    def load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        try:
            os.makedirs(self.docs_dir, exist_ok=True)
            # Use Path to normalize/resolve the path to avoid "Invalid argument" errors on Windows
            path = Path(self.filepath).resolve()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"\n  [!] Error saving index to {self.filepath}: {e}")

    def add_video(self, video_id, section, source_page, thread_title=None, nickname=None):
        if video_id in self.excluded_ids:
            # Skip excluded videos silently during scanning
            return

        if video_id not in self.data:
            self.data[video_id] = {
                "url": canonical_yt_url(video_id),
                "title": None,
                "description": None,
                "channel_name": None,
                "channel_url": None,
                "publish_date": None,
                "view_count": None,
                "like_count": None,
                "tags": [],
                "nickname": None,
                "sections": [],
                "source_pages": [],
                "thread_titles": [],
                "status": "pending",
                "local_file": None,
                "mirrors": None,
            }
        e = self.data[video_id]
        if section not in e["sections"]:
            e["sections"].append(section)
        if source_page not in e["source_pages"]:
            e["source_pages"].append(source_page)
        if thread_title and thread_title not in e.get("thread_titles", []):
            e.setdefault("thread_titles", []).append(thread_title)
        if nickname and not e.get("nickname"):
            e["nickname"] = nickname

    def needs_metadata(self, video_id):
        e = self.data.get(video_id, {})
        
        # Don't try to fetch data for videos we know are dead/removed
        if e.get("status") == "unavailable":
            return False
            
        # Catch known yt-dlp error artifact
        if e.get("title") == "warnings.warn(":
            return True
            
        # Check if ANY of the primary metadata or stats fields are missing (None)
        return (e.get("title") is None or
                e.get("description") is None or
                e.get("channel_name") is None or
                e.get("channel_url") is None or
                e.get("publish_date") is None or
                e.get("view_count") is None or
                e.get("like_count") is None)

    def set_metadata(self, video_id, title=None, description=None,
                     channel_name=None, channel_url=None,
                     publish_date=None, view_count=None, like_count=None, tags=None):
        if video_id not in self.data:
            return
        e = self.data[video_id]
        if title:
            e["title"] = title
        if description is not None:
            e["description"] = description
        if channel_name:
            e["channel_name"] = channel_name
        if channel_url:
            e["channel_url"] = channel_url
        if publish_date is not None:
            e["publish_date"] = publish_date
        if view_count is not None:
            e["view_count"] = view_count
        if like_count is not None:
            e["like_count"] = like_count
        if tags is not None:
            e["tags"] = tags

    def is_done(self, vid):
        return self.data.get(vid, {}).get("status") in ("downloaded", "unavailable")

    def set_downloaded(self, vid, local_file, title=None):
        if vid in self.data:
            e = self.data[vid]
            e["status"] = "downloaded"
            e["local_file"] = local_file
            if title:
                e["title"] = title

    def set_unavailable(self, vid):
        if vid in self.data:
            self.data[vid]["status"] = "unavailable"

    def set_failed(self, vid):
        if vid in self.data:
            self.data[vid]["status"] = "failed"

    def clear_failed(self):
        for e in self.data.values():
            if e["status"] == "failed":
                e["status"] = "pending"

    def pending(self):
        return [vid for vid, e in self.data.items() 
                if e["status"] == "pending" and vid not in self.excluded_ids]

    def stats(self):
        s = {"total": 0, "downloaded": 0, "unavailable": 0, "failed": 0, "pending": 0}
        for e in self.data.values():
            s["total"] += 1
            key = e.get("status", "pending")
            s[key] = s.get(key, 0) + 1
        return s

    def remove_disallowed_channels(self):
        to_remove = [vid for vid, e in self.data.items()
                     if is_disallowed_channel(e.get("channel_name"))]
        for vid in to_remove:
            del self.data[vid]
        return len(to_remove)


# ── Scan Cache ───────────────────────────────────────────────────────────────

class ScanCache:
    """
    Tracks which HTML pages have already been scanned.
    {
      "rel/path/to/page.html": {
        "scanned_at": "2025-01-01T00:00:00",
        "video_ids":  ["id1", "id2"],
        "new_count":  2
      }
    }
    """

    def __init__(self, video_dir):
        self.filepath = os.path.join(video_dir, "scan_cache.json")
        self.data = {}

    def load(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def is_scanned(self, rel_path):
        return rel_path in self.data

    def mark_scanned(self, rel_path, video_ids, new_count):
        self.data[rel_path] = {
            "scanned_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "video_ids":  list(video_ids),
            "new_count":  new_count,
        }


# ── Scanner ───────────────────────────────────────────────────────────────────

class Scanner:

    def __init__(self, site_dir):
        self.site_dir = site_dir

    def scan_file(self, filepath):
        """Returns (set_of_video_ids, nickname_or_None)."""
        try:
            content = Path(filepath).read_text(encoding="utf-8", errors="replace")
        except Exception:
            return set(), None
        ids = set()
        for pat in YT_PATTERNS:
            for m in pat.finditer(content):
                vid = extract_video_id(m.group(0))
                if vid:
                    ids.add(vid)
        nickname = None
        try:
            soup = BeautifulSoup(content, "lxml")
            nick_tag = soup.find(class_="nick")
            if nick_tag:
                nickname = nick_tag.get_text(strip=True)
            for tag in soup.find_all(["a", "iframe", "embed", "object", "source", "param"]):
                for attr in ("href", "src", "data", "value"):
                    vid = extract_video_id(tag.get(attr, ""))
                    if vid:
                        ids.add(vid)
        except Exception:
            pass
        return ids, nickname

    def scan_sections(self, index, scan_cache=None, save_fn=None, save_interval=10):
        new_found = 0
        file_count = 0
        skipped = 0
        for sec in SCAN_SECTIONS:
            sec_dir = os.path.join(self.site_dir, sec)
            if not os.path.isdir(sec_dir):
                print(f"  [!] Directory not found: {sec_dir}")
                continue
            html_files = []
            for root, _, files in os.walk(sec_dir):
                for fname in files:
                    if fname.endswith((".html", ".htm")):
                        html_files.append(os.path.join(root, fname))

            print(f"  {sec}: {len(html_files)} HTML files", flush=True)
            for fpath in html_files:
                rel = os.path.relpath(fpath, self.site_dir)

                if scan_cache and scan_cache.is_scanned(rel):
                    skipped += 1
                    continue

                fname = os.path.basename(fpath)
                if re.match(r'^page_\d+\.html$', fname):
                    parent = os.path.basename(os.path.dirname(fpath))
                    thread_title = thread_title_from_filename(parent)
                else:
                    thread_title = thread_title_from_filename(fname)

                ids, nickname = self.scan_file(fpath)
                new_this_file = 0
                for vid in ids:
                    was_new = vid not in index.data
                    index.add_video(vid, sec, rel, thread_title, nickname=nickname)
                    if was_new:
                        new_found += 1
                        new_this_file += 1

                if scan_cache:
                    scan_cache.mark_scanned(rel, ids, new_this_file)

                if ids:
                    print(f"  [scan] {rel}  → {len(ids)} video(s) found, {new_this_file} new", flush=True)
                else:
                    print(f"  [scan] {rel}  → no videos", flush=True)

                file_count += 1
                if save_fn and file_count % save_interval == 0:
                    save_fn()
                    if scan_cache:
                        scan_cache.save()

        if skipped:
            print(f"  (skipped {skipped} already-scanned pages)")
        return new_found


# ── YouTube metadata ──────────────────────────────────────────────────────────

def fetch_yt_metadata(video_id):
    """
    Run yt-dlp --dump-json to get title, description, channel info, tags.
    Returns dict | 'unavailable' | None (temp error)
    """
    url = canonical_yt_url(video_id)
    try:
        r = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-playlist",
             "--socket-timeout", "20", url],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0 and r.stdout.strip():
            raw = next(
                (l for l in reversed(r.stdout.splitlines()) if l.strip().startswith("{")),
                None,
            )
            if raw is None:
                return None
            d = json.loads(raw)
            raw_date = d.get("upload_date")  # "20230415"
            publish_date = None
            if raw_date and len(raw_date) == 8:
                publish_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            return {
                "title":        d.get("title"),
                "description":  (d.get("description") or "")[:3000],
                "channel_name": d.get("uploader") or d.get("channel"),
                "channel_url":  d.get("uploader_url") or d.get("channel_url"),
                "publish_date": publish_date,
                "view_count":   d.get("view_count"),
                "like_count":   d.get("like_count"),
                "tags":         d.get("tags") or [],
            }
        combined = (r.stdout + r.stderr).lower()
        for msg in UNAVAIL_MSGS:
            if msg in combined:
                return "unavailable"
    except Exception:
        pass
    return None


# ── Downloader ────────────────────────────────────────────────────────────────

def download_video(video_id, output_dir, yt_format, rate_limit,
                   current_num, total_num):
    """
    Download one video with a real-time per-video progress bar.
    Returns ('ok'|'exists'|'unavailable'|'error', local_file, title)
    """
    url = canonical_yt_url(video_id)
    os.makedirs(output_dir, exist_ok=True)
    outtmpl = os.path.join(output_dir, "%(title).80s - %(id)s.%(ext)s")

    cmd = [
        "yt-dlp",
        "--no-playlist", "--no-overwrites",
        "--write-thumbnail", "--convert-thumbnails", "jpg",
        "--embed-thumbnail", "--add-metadata",
        "--newline",
        "--print", "after_move:filepath",
        "--print", "%(title)s",
        "--format", yt_format,
        "--output", outtmpl,
        "--retries", "3",
        "--socket-timeout", "30",
        "--no-warnings",
    ]
    if rate_limit:
        cmd += ["--limit-rate", rate_limit]
    cmd.append(url)

    local_file = None
    title = None
    is_exists = False

    overall_pct = (current_num - 1) / total_num * 100
    ov_bar = bar(overall_pct, 15)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue

            line = ANSI_RE.sub('', line)

            if "has already been downloaded" in line:
                is_exists = True
                continue



            stripped = line.strip()
            if re.match(r'.+\.py:\d+: \w+Warning:', stripped):
                continue
            if stripped.startswith("warnings.warn("):
                continue
            if stripped and not stripped.startswith("["):
                if (os.sep in stripped or "/" in stripped) and any(
                    stripped.endswith(e)
                    for e in (".mp4", ".mkv", ".webm", ".mp3", ".m4a", ".opus")
                ):
                    local_file = stripped
                elif not title:
                    title = stripped

        proc.wait()
        clear_line()

        if not local_file:
            matches = [
                m for m in glob.glob(os.path.join(output_dir, f"* - {video_id}.*"))
                if not m.endswith((".jpg", ".png", ".webp"))
            ]
            if matches:
                local_file = matches[0]

        if is_exists:
            return "exists", local_file, title
        if proc.returncode == 0:
            return "ok", local_file, title

        return "error", None, None

    except subprocess.TimeoutExpired:
        proc.kill()
        clear_line()
        return "error", None, None
    except Exception as ex:
        clear_line()
        print(f"  [!] {ex}")
        return "error", None, None


# ── Interactive phases ────────────────────────────────────────────────────────

def do_update_index(index):
    # Skip scanning HTML pages as they are already scraped
    print("  Skipping HTML scan (all pages already scraped).")

    removed = index.remove_disallowed_channels()
    if removed:
        print(f"  Removed {removed} video(s) from disallowed channels.")
        index.save()

    st = index.stats()
    print(f"  Total videos in index: {st['total']}")
    print()

    need_meta = [vid for vid in index.data if index.needs_metadata(vid) and vid not in index.excluded_ids]

    if not need_meta:
        print("  All videos already have metadata.")
        return

    total_meta = len(need_meta)
    print(f"  Fetching YouTube metadata for {total_meta} videos")
    print(f"  (title, description, channel link, tags)...")
    print()

    for i, vid in enumerate(need_meta, 1):
        overall_pct = i / total_meta * 100
        ov_bar = bar(overall_pct, 30)
        print(f"\r  {ov_bar}  {i}/{total_meta}", end="", flush=True)

        meta = fetch_yt_metadata(vid)
        if meta == "unavailable":
            index.set_unavailable(vid)
        elif meta:
            index.set_metadata(vid, **meta)

        if i % 20 == 0:
            index.save()

    clear_line()
    index.save()

    st = index.stats()
    print(f"  Done — index updated.")
    print(f"  Total: {st['total']}  Pending: {st['pending']}  "
          f"Unavailable: {st['unavailable']}")


def do_download(index, video_dir, yt_format, rate_limit, retry_failed):
    if retry_failed:
        index.clear_failed()
        index.save()
        print("  Cleared failed status — will retry.\n")

    pending = index.pending()
    if not pending:
        print("  Nothing to download — either run 'Update index' first")
        print("  or everything is already downloaded / unavailable.")
        return

    total = len(pending)
    print(f"  {total} videos pending.\n")

    ok_count = skip_count = unavail_count = err_count = 0

    for i, vid in enumerate(pending, 1):
        try:
            e = index.data[vid]
            
            if e.get("title") == "warnings.warn(":
                meta = fetch_yt_metadata(vid)
                if isinstance(meta, dict) and meta.get("title"):
                    e["title"] = meta["title"]
                    index.save()
                    
            sec = e["sections"][0] if e["sections"] else "Unknown"
            thread = (e.get("thread_titles") or [""])[0] or vid
            yt_title = e.get("title") or vid

            ch_name = e.get("channel_name")
            folder_name = safe_filename(ch_name) if ch_name else "Unknown Channel"
            out_dir = os.path.join(video_dir, folder_name)

            print(f"  [{i}/{total}] {thread[:60]}")
            if e.get("channel_name"):
                ch_url = e.get("channel_url", "")
                print(f"  Channel: {e['channel_name']}  {ch_url}")
            print(f"  URL:     {canonical_yt_url(vid)}")

            status, local_file, dl_title = download_video(
                vid, out_dir, yt_format, rate_limit, i, total,
            )

            if status == "ok":
                rel = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel, dl_title)
                print(f"  ✓ {os.path.basename(local_file or '')}", flush=True)
                ok_count += 1
            elif status == "exists":
                if not index.is_done(vid):
                    rel = os.path.relpath(local_file, ".") if local_file else None
                    index.set_downloaded(vid, rel, dl_title)
                print(f"  = already downloaded")
                skip_count += 1
            elif status == "unavailable":
                index.set_unavailable(vid)
                print("  ⊘ Unavailable (removed / private)")
                unavail_count += 1
            else:
                index.set_failed(vid)
                print("  ✗ Failed")
                err_count += 1

            index.save()

            if status == "ok":
                time.sleep(1)
        except Exception as ex:
            print(f"\n  [!] Unexpected error processing {vid}: {ex}")
            err_count += 1

    print("─" * 54)
    print(f"  Downloaded:  {ok_count}")
    print(f"  Skipped:     {skip_count}  (already on disk)")
    print(f"  Unavailable: {unavail_count}  (removed / private)")
    print(f"  Failed:      {err_count}  (re-run to retry)")
    print(f"  Index:       {os.path.abspath(index.filepath)}")


def do_scrape_channels(index):
    """Scans ALLOWED_CHANNELS for new YTP videos matching keywords and logs details with a progress bar."""
    
    if not ALLOWED_CHANNELS:
        print("  No allowed channels defined to scrape.")
        return
        
    total_channels = len(ALLOWED_CHANNELS)
    print(f"  Found {total_channels} allowed channel(s) to scrape.")
    new_total = 0
    
    for i, ch_url in enumerate(ALLOWED_CHANNELS, 1):
        print(f"\n  Scraping Channel [{i}/{total_channels}]: {ch_url}")
        videos_url = channel_videos_url(ch_url)
        nocoldiz = is_nocoldiz_channel(ch_url)
        
        try:
            # Use flat-playlist to quickly get the list of video IDs and titles
            r = subprocess.run(
                ["yt-dlp", "--flat-playlist", "--dump-json", "--no-warnings", 
                 "--socket-timeout", "20", videos_url],
                capture_output=True, text=True, timeout=120,
            )
            
            if r.returncode == 0 and r.stdout.strip():
                lines = [l for l in r.stdout.splitlines() if l.strip().startswith("{")]
                total_videos = len(lines)
                
                for v_idx, line in enumerate(lines, 1):
                    # Progress bar for the videos in the current channel
                    v_pct = v_idx / total_videos * 100
                    p_bar = bar(v_pct, 30)
                    print(f"\r    {p_bar} {v_idx}/{total_videos} videos scanned", end="", flush=True)

                    try:
                        d = json.loads(line)
                        vid = d.get("id")
                        title = d.get("title", "")
                        
                        # Match logic based on CHANNEL_KEYWORDS or NOCOLDIZ_BLACKLIST
                        is_match = False
                        if nocoldiz:
                            if not NOCOLDIZ_BLACKLIST.search(title):
                                is_match = True
                        elif CHANNEL_KEYWORDS.search(title):
                            is_match = True

                        # If it matches and is not already in the index (and not excluded), log and add it
                        if is_match and vid and vid not in index.data and vid not in index.excluded_ids:
                            clear_line()
                            print(f"    [+] New keyword match found: {title} ({vid})")
                            index.add_video(vid, "Scraped Channel", videos_url, title)
                            index.set_metadata(vid, title=title, channel_url=ch_url)
                            new_total += 1
                            
                    except json.JSONDecodeError:
                        continue
                        
                clear_line()
                print(f"    Done scanning {total_videos} videos.")
            else:
                clear_line()
                print(f"    [!] No videos found or yt-dlp returned an error for {ch_url}")
                
        except subprocess.TimeoutExpired:
            clear_line()
            print(f"    [!] Timeout scraping {ch_url}")
        except Exception as e:
            clear_line()
            print(f"    [!] Error scraping {ch_url}: {e}")

    # Save all new entries to the index
    if new_total > 0:
        index.save()
        
    print(f"\n  Finished scraping channels. Added {new_total} new videos to the index.")
def do_download_youtube(index, video_dir, yt_format, rate_limit, retry_failed):
    if retry_failed:
        for e in index.data.values():
            if "Youtube" in e.get("sections", []) and e["status"] == "failed":
                e["status"] = "pending"
        index.save()
        print("  Cleared failed status for 'Youtube' section — will retry.\n")

    pending = [
        vid for vid, e in index.data.items()
        if "Youtube" in e.get("sections", []) and e["status"] == "pending"
        and vid not in index.excluded_ids
    ]

    if not pending:
        print("  Nothing to download in 'Youtube' section.")
        print("  Run 'Scrape channels' first, or everything is already downloaded.")
        return

    total = len(pending)
    print(f"  {total} 'Youtube' section video(s) pending.\n")

    ok_count = skip_count = unavail_count = err_count = 0

    for i, vid in enumerate(pending, 1):
        try:
            e = index.data[vid]
            
            if e.get("title") == "warnings.warn(":
                meta = fetch_yt_metadata(vid)
                if isinstance(meta, dict) and meta.get("title"):
                    e["title"] = meta["title"]
                    index.save()
                    
            label = (e.get("thread_titles") or [""])[0] or e.get("title") or vid

            ch_name = e.get("channel_name")
            folder_name = safe_filename(ch_name) if ch_name else "Unknown Channel"
            out_dir = os.path.join(video_dir, folder_name)

            print(f"  [{i}/{total}] {label[:60]}")
            if e.get("channel_name"):
                print(f"  Channel: {e['channel_name']}  {e.get('channel_url', '')}")
            print(f"  URL:     {canonical_yt_url(vid)}")

            status, local_file, dl_title = download_video(
                vid, out_dir, yt_format, rate_limit, i, total,
            )

            if status == "ok":
                rel = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel, dl_title)
                print(f"  ✓ {os.path.basename(local_file or '')}")
                ok_count += 1
            elif status == "exists":
                if not index.is_done(vid):
                    rel = os.path.relpath(local_file, ".") if local_file else None
                    index.set_downloaded(vid, rel, dl_title)
                print(f"  = already downloaded")
                skip_count += 1
            elif status == "unavailable":
                index.set_unavailable(vid)
                print("  ⊘ Unavailable (removed / private)")
                unavail_count += 1
            else:
                index.set_failed(vid)
                print("  ✗ Failed")
                err_count += 1

            index.save()

            if status == "ok":
                time.sleep(1)
        except Exception as ex:
            print(f"\n  [!] Unexpected error processing {vid}: {ex}")
            err_count += 1

    print("─" * 54)
    print(f"  Downloaded:  {ok_count}")
    print(f"  Skipped:     {skip_count}  (already on disk)")
    print(f"  Unavailable: {unavail_count}  (removed / private)")
    print(f"  Failed:      {err_count}  (re-run to retry)")
    print(f"  Index:       {os.path.abspath(index.filepath)}")


def do_download_italian(index, video_dir, yt_format, rate_limit, retry_failed, year_limit=2018):
    def is_italian(e):
        # Must match keywords
        title = e.get("title") or ""
        if not CHANNEL_KEYWORDS.search(title):
            return False

        secs = e.get("sections", [])
        #TODO: reenable this after scraping all historic ones in secs 
        # 
        if "YTP fai da te" in secs or "YTP nostrane" in secs or "Scraped Channel" in secs or "Youtube" in secs:
            return True
        ch_url = e.get("channel_url", "")
        if ch_url:
            norm_ch = ch_url.rstrip("/").replace("/featured", "").lower()
            for ac in ALLOWED_CHANNELS:
                norm_ac = ac.rstrip("/").replace("/featured", "").lower()
                if norm_ac in norm_ch or norm_ch in norm_ac:
                    return True
        return False

    def is_in_year_range(e):
        if year_limit is None:
            return True
        pub_date = e.get("publish_date")
        if not pub_date:
            return False
        try:
            # publish_date is "YYYY-MM-DD"
            year = int(pub_date.split("-")[0])
            return year <= year_limit
        except (ValueError, IndexError):
            return False

    if retry_failed:
        for e in index.data.values():
            if is_italian(e) and is_in_year_range(e) and e["status"] == "failed":
                e["status"] = "pending"
        index.save()
        print(f"  Cleared failed status for Italian YTPs (until {year_limit}) — will retry.\n")

    pending = [
        vid for vid, e in index.data.items()
        if is_italian(e) and is_in_year_range(e) and e["status"] == "pending"
        and vid not in index.excluded_ids
    ]

    if not pending:
        print(f"  Nothing to download for Italian YTPs (until {year_limit}).")
        return

    total = len(pending)
    print(f"  {total} Italian YTP video(s) pending (until {year_limit}).\n")

    ok_count = skip_count = unavail_count = err_count = 0

    for i, vid in enumerate(pending, 1):
        try:
            e = index.data[vid]
            
            if e.get("title") == "warnings.warn(":
                meta = fetch_yt_metadata(vid)
                if isinstance(meta, dict) and meta.get("title"):
                    e["title"] = meta["title"]
                    index.save()
                    
            sec = e["sections"][0] if e["sections"] else "Unknown"
            label = (e.get("thread_titles") or [""])[0] or e.get("title") or vid
            
            ch_name = e.get("channel_name")
            folder_name = safe_filename(ch_name) if ch_name else "Unknown Channel"
            out_dir = os.path.join(video_dir, folder_name)

            print(f"  [{i}/{total}] {label[:60]}")
            if e.get("channel_name"):
                print(f"  Channel: {e['channel_name']}  {e.get('channel_url', '')}")
            print(f"  URL:     {canonical_yt_url(vid)}")

            status, local_file, dl_title = download_video(
                vid, out_dir, yt_format, rate_limit, i, total,
            )

            if status == "ok":
                rel = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel, dl_title)
                print(f"  ✓ {os.path.basename(local_file or '')}")
                ok_count += 1
            elif status == "exists":
                if not index.is_done(vid):
                    rel = os.path.relpath(local_file, ".") if local_file else None
                    index.set_downloaded(vid, rel, dl_title)
                print(f"  = already downloaded")
                skip_count += 1
            elif status == "unavailable":
                index.set_unavailable(vid)
                print("  ⊘ Unavailable (removed / private)")
                unavail_count += 1
            else:
                index.set_failed(vid)
                print("  ✗ Failed")
                err_count += 1

            index.save()

            if status == "ok":
                time.sleep(1)
        except Exception as ex:
            print(f"\n  [!] Unexpected error processing {vid}: {ex}")
            err_count += 1

    print("─" * 54)
    print(f"  Downloaded:  {ok_count}")
    print(f"  Skipped:     {skip_count}  (already on disk)")
    print(f"  Unavailable: {unavail_count}  (removed / private)")
    print(f"  Failed:      {err_count}  (re-run to retry)")
    print(f"  Index:       {os.path.abspath(index.filepath)}")



def do_download_risorse(index, video_dir, yt_format, rate_limit, retry_failed):
    target_sections = {"Risorse", "Old sources"}
    
    if retry_failed:
        for e in index.data.values():
            if any(s in target_sections for s in e.get("sections", [])) and e["status"] == "failed":
                e["status"] = "pending"
        index.save()
        print("  Cleared failed status for 'Risorse' & 'Old sources' — will retry.\n")

    pending = [
        vid for vid, e in index.data.items()
        if any(s in target_sections for s in e.get("sections", [])) and e["status"] == "pending"
        and vid not in index.excluded_ids
    ]

    if not pending:
        print("  Nothing to download in 'Risorse' or 'Old sources' sections.")
        return

    total = len(pending)
    print(f"  {total} Risorse/Old sources video(s) pending.\n")

    ok_count = skip_count = unavail_count = err_count = 0

    for i, vid in enumerate(pending, 1):
        try:
            e = index.data[vid]

            # Determine output directory based on channel name
            ch_name = e.get("channel_name")
            folder_name = safe_filename(ch_name) if ch_name else "Unknown Channel"
            out_dir = os.path.join(video_dir, folder_name)
            os.makedirs(out_dir, exist_ok=True)
            
            if e.get("title") == "warnings.warn(":
                meta = fetch_yt_metadata(vid)
                if isinstance(meta, dict) and meta.get("title"):
                    e["title"] = meta["title"]
                    index.save()
                    
            label = (e.get("thread_titles") or [""])[0] or e.get("title") or vid

            print(f"  [{i}/{total}] {label[:60]}")
            if e.get("channel_name"):
                print(f"  Channel: {e['channel_name']}  {e.get('channel_url', '')}")
            print(f"  URL:     {canonical_yt_url(vid)}")

            status, local_file, dl_title = download_video(
                vid, out_dir, yt_format, rate_limit, i, total,
            )

            if status == "ok":
                rel = os.path.relpath(local_file, ".") if local_file else None
                index.set_downloaded(vid, rel, dl_title)
                print(f"  ✓ {os.path.basename(local_file or '')}")
                ok_count += 1
            elif status == "exists":
                if not index.is_done(vid):
                    rel = os.path.relpath(local_file, ".") if local_file else None
                    index.set_downloaded(vid, rel, dl_title)
                print(f"  = already downloaded")
                skip_count += 1
            elif status == "unavailable":
                index.set_unavailable(vid)
                print("  ⊘ Unavailable (removed / private)")
                unavail_count += 1
            else:
                index.set_failed(vid)
                print("  ✗ Failed")
                err_count += 1

            index.save()

            if status == "ok":
                time.sleep(1)
        except Exception as ex:
            print(f"\n  [!] Unexpected error processing {vid}: {ex}")
            err_count += 1

    print("─" * 54)
    print(f"  Downloaded:  {ok_count}")
    print(f"  Skipped:     {skip_count}  (already on disk)")
    print(f"  Unavailable: {unavail_count}  (removed / private)")
    print(f"  Failed:      {err_count}  (re-run to retry)")
    print(f"  Index:       {os.path.abspath(index.filepath)}")


def do_stats(index, output_path="stats.md"):

    from collections import defaultdict

    if not index.data:
        print("  Index is empty. Run 'Update index' first.")
        return

    # Only count videos in SCAN_SECTIONS
    filtered = {
        vid: e for vid, e in index.data.items()
        if any(s in SCAN_SECTIONS for s in e.get("sections", []))
    }

    if not filtered:
        print("  No videos found in scan sections.")
        return

    # Per-section counts (a video in multiple sections is counted in each)
    section_stats = {sec: {"total": 0, "downloaded": 0, "unavailable": 0,
                            "pending": 0, "failed": 0}
                     for sec in SCAN_SECTIONS}
    for e in filtered.values():
        status = e.get("status", "pending")
        for sec in e.get("sections", []):
            if sec in section_stats:
                section_stats[sec]["total"] += 1
                section_stats[sec][status] = section_stats[sec].get(status, 0) + 1

    # Grand totals (unique video count)
    grand = {"total": len(filtered), "downloaded": 0, "unavailable": 0,
             "pending": 0, "failed": 0}
    for e in filtered.values():
        st = e.get("status", "pending")
        grand[st] = grand.get(st, 0) + 1

    grand_pct = (f"{grand['unavailable'] / grand['total'] * 100:.1f}%"
                 if grand["total"] else "—")

    # Per-channel table (only channels from SCAN_SECTIONS videos)
    channels = defaultdict(lambda: {"total": 0, "downloaded": 0, "unavailable": 0,
                                     "pending": 0, "failed": 0, "sections": set()})
    for e in filtered.values():
        name = e.get("channel_name") or "(unknown)"
        ch = channels[name]
        ch["total"] += 1
        ch[e.get("status", "pending")] += 1
        for s in e.get("sections", []):
            if s in SCAN_SECTIONS:
                ch["sections"].add(s)

    rows = sorted(channels.items(), key=lambda x: x[1]["total"], reverse=True)

    # Build markdown
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    md = [f"# YTP Backup Stats", f"", f"Generated: {now}", ""]

    md += ["## Sections", ""]
    md += ["| Section | Total | Downloaded | Unavailable | % N/A | Pending | Failed |"]
    md += ["|---|---|---|---|---|---|---|"]
    for sec in SCAN_SECTIONS:
        s = section_stats[sec]
        t = s["total"]
        u = s["unavailable"]
        pct = f"{u / t * 100:.1f}%" if t else "—"
        md.append(f"| {sec} | {t} | {s['downloaded']} | {u} | {pct} | {s['pending']} | {s['failed']} |")
    md.append(
        f"| **Total** | **{grand['total']}** | **{grand['downloaded']}** | "
        f"**{grand['unavailable']}** | **{grand_pct}** | **{grand['pending']}** | **{grand['failed']}** |"
    )
    md += [""]

    md += ["## Channels", ""]
    md += ["| Channel | Total | DL | N/A | % N/A | Pending | Failed | Sections |"]
    md += ["|---|---|---|---|---|---|---|---|"]
    for name, c in rows:
        t = c["total"]
        u = c["unavailable"]
        pct = f"{u / t * 100:.1f}%" if t else "—"
        secs = ", ".join(sorted(c["sections"]))
        n = name.replace("|", "\\|")
        md.append(f"| {n} | {t} | {c['downloaded']} | {u} | {pct} | {c['pending']} | {c['failed']} | {secs} |")
    md += [""]

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print(f"  Stats written → {os.path.abspath(output_path)}")

    # Print summary to terminal
    print(f"\n  {'Section':<32} {'Total':>5}  {'N/A':>4}  {'% N/A':>6}")
    print("  " + "-" * 54)
    for sec in SCAN_SECTIONS:
        s = section_stats[sec]
        t = s["total"]
        u = s["unavailable"]
        pct = f"{u / t * 100:.1f}%" if t else "—"
        print(f"  {sec:<32} {t:>5}  {u:>4}  {pct:>6}")
    print("  " + "-" * 54)
    print(f"  {'TOTAL':<32} {grand['total']:>5}  {grand['unavailable']:>4}  {grand_pct:>6}")
    print()


def do_chronology(index, top_n=20):
    if not index.data:
        print("  Index is empty. Run 'Update index' first.")
        return

    candidates = [
        e for e in index.data.values()
        if e.get("status") != "unavailable"
        and e.get("title")
        and e.get("title") != "warnings.warn("
        and e.get("view_count") is not None
    ]

    if not candidates:
        print("  No view count data yet. Run 'Update index' to fetch metadata.")
        return

    top = sorted(candidates, key=lambda e: e.get("view_count") or 0, reverse=True)[:top_n]
    top.sort(key=lambda e: e.get("publish_date") or "")

    col_title   = 40
    col_channel = 22
    header = (f"  {'#':>3}  {'Year':<4}  {'Title':<{col_title}}  "
              f"{'Channel':<{col_channel}}  {'Views':>10}  {'Likes':>8}")
    sep    = "  " + "-" * (len(header) - 2)

    print()
    print(header)
    print(sep)
    for rank, e in enumerate(top, 1):
        year    = (e.get("publish_date") or "????")[:4]
        title   = (e.get("title") or "")[:col_title]
        channel = (e.get("channel_name") or "")[:col_channel]
        views   = e.get("view_count")
        likes   = e.get("like_count")
        views_s = f"{views:,}" if views is not None else "—"
        likes_s = f"{likes:,}" if likes is not None else "—"
        print(f"  {rank:>3}  {year:<4}  {title:<{col_title}}  "
              f"{channel:<{col_channel}}  {views_s:>10}  {likes_s:>8}")
    print(sep)
    print(f"  Top {len(top)} most-viewed videos (of {len(candidates)} with view data), sorted by year")
    print()


def _fmt_views_it(n):
    """Italian-style compact view count: 1,2 mln / 310K / 5 / —"""
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        v = n / 1_000_000_000
        s = f"{v:.1f}".replace(".", ",")
        return f"{s} mrd" if v != int(v) else f"{int(v)} mrd"
    if n >= 1_000_000:
        v = n / 1_000_000
        s = f"{v:.1f}".replace(".", ",")
        return f"{s} mln" if v != int(v) else f"{int(v)} mln"
    if n >= 1_000:
        v = n / 1_000
        s = f"{v:.1f}".replace(".", ",")
        return f"{s}K" if v != int(v) else f"{int(v)}K"
    return str(n)


def do_find_mirrors(index):
    """Search YouTube for reuploads of unavailable videos."""
    candidates = [
        (vid, e) for vid, e in index.data.items()
        if e.get("status") == "unavailable" and not e.get("mirrors")
        and vid not in index.excluded_ids
    ]

    if not candidates:
        print("  No unavailable videos without mirror data found.")
        return

    total = len(candidates)
    print(f"  Searching for mirrors of {total} unavailable video(s)...")
    print()

    found_count = 0

    for i, (vid, e) in enumerate(candidates, 1):
        pct = i / total * 100
        pb = bar(pct, 28)
        print(f"\r  {pb}  {i}/{total}  found={found_count}  ", end="", flush=True)

        title = e.get("title")
        thread_title = (e.get("thread_titles") or [None])[0]

        # Try video title first, fall back to thread title
        queries = [q for q in [title, thread_title] if q and q.strip()]
        if not queries:
            e["mirrors"] = []
            continue

        mirrors = []
        for query in queries:
            query_safe = query[:100]
            try:
                r = subprocess.run(
                    ["yt-dlp", f"ytsearch5:{query_safe}",
                     "--flat-playlist", "--dump-json",
                     "--no-warnings", "--socket-timeout", "20"],
                    capture_output=True, text=True, timeout=60,
                )
                for line in r.stdout.splitlines():
                    if not line.strip().startswith("{"):
                        continue
                    try:
                        d = json.loads(line)
                        found_vid = d.get("id")
                        found_title = d.get("title") or ""
                        if found_vid and found_vid != vid:
                            mirrors.append({
                                "id":           found_vid,
                                "title":        found_title,
                                "url":          canonical_yt_url(found_vid),
                                "search_query": query_safe,
                            })
                    except json.JSONDecodeError:
                        continue
            except (subprocess.TimeoutExpired, Exception):
                continue

            if mirrors:
                break  # found results on first query, no need for fallback

        e["mirrors"] = mirrors
        if mirrors:
            found_count += 1

        if i % 10 == 0:
            index.save()

    clear_line()
    index.save()
    print(f"  Found potential mirrors for {found_count} / {total} unavailable videos.")
    print(f"  Mirror data stored in 'mirrors' field of video_index.json.")


def do_scrape_profiles(index, docs_dir):
    """Scrape channel profiles (name, description, thumbnail, subscribers, date) for all unique channels."""
    if not index.data:
        print("  Index is empty. Run 'Update index' first.")
        return

    # Collect unique channels from the index
    channel_map = {}  # channel_url -> channel_name
    for e in index.data.values():
        ch_url = e.get("channel_url")
        ch_name = e.get("channel_name")
        if ch_url and ch_name and ch_url not in channel_map:
            channel_map[ch_url] = ch_name

    if not channel_map:
        print("  No channels with URLs found in the index.")
        return

    # Load existing data to allow incremental updates
    output_path = os.path.join(docs_dir, "ytpoopers.json")
    existing = {}
    if os.path.exists(output_path):
        try:
            with open(output_path, encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = {}

    thumb_dir = os.path.join(docs_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    total = len(channel_map)
    print(f"  Found {total} unique channel(s) to scrape.")
    print(f"  Already scraped channels will be skipped.")
    print()

    scraped = skipped = failed = 0

    for i, (ch_url, ch_name) in enumerate(channel_map.items(), 1):
        pct = i / total * 100
        pb = bar(pct, 26)
        print(f"\r  {pb}  {i}/{total}  ok={scraped} skip={skipped} fail={failed}  ",
              end="", flush=True)

        # Skip if already scraped
        if ch_url in existing and existing[ch_url].get("description") is not None:
            skipped += 1
            continue

        # Use yt-dlp to get channel about page metadata
        about_url = ch_url.rstrip("/")
        about_url = re.sub(r'/(videos|shorts|streams|playlists|about|community|featured)$', '', about_url)

        try:
            r = subprocess.run(
                ["yt-dlp", "--dump-json", "--playlist-items", "0",
                 "--no-warnings", "--socket-timeout", "20", about_url],
                capture_output=True, text=True, timeout=60,
            )

            profile = {
                "channel_name": ch_name,
                "channel_url": ch_url,
                "description": None,
                "subscriber_count": None,
                "creation_date": None,
                "thumbnail": None,
            }

            if r.returncode == 0 and r.stdout.strip():
                raw = next(
                    (l for l in reversed(r.stdout.splitlines()) if l.strip().startswith("{")),
                    None,
                )
                if raw:
                    d = json.loads(raw)
                    profile["channel_name"] = d.get("uploader") or d.get("channel") or ch_name
                    profile["description"] = d.get("description") or ""
                    profile["subscriber_count"] = d.get("channel_follower_count")

                    # Upload date of first video as proxy for channel creation
                    raw_date = d.get("upload_date")
                    if raw_date and len(raw_date) == 8:
                        profile["creation_date"] = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

                    # Download channel thumbnail
                    thumb_url = None
                    thumbnails = d.get("thumbnails") or []
                    # Try channel-level avatar from uploader_url or pick last thumbnail
                    if thumbnails:
                        thumb_url = thumbnails[-1].get("url")

                    if not thumb_url:
                        # Try channel_thumbnails field (some yt-dlp versions)
                        for t in (d.get("channel_thumbnails") or []):
                            thumb_url = t.get("url")

                    if thumb_url:
                        safe_name = re.sub(r'[<>:"/\\|?*]', '_', ch_name)[:60]
                        thumb_ext = "jpg"
                        thumb_file = os.path.join(thumb_dir, f"{safe_name}.{thumb_ext}")
                        try:
                            urllib.request.urlretrieve(thumb_url, thumb_file)
                            profile["thumbnail"] = f"thumbnails/{safe_name}.{thumb_ext}"
                        except Exception:
                            pass

            existing[ch_url] = profile
            scraped += 1

        except subprocess.TimeoutExpired:
            failed += 1
        except Exception:
            failed += 1

        # Save periodically
        if i % 10 == 0:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)

        time.sleep(0.5)

    clear_line()

    # Final save
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    print(f"  Done. Scraped: {scraped}  Skipped: {skipped}  Failed: {failed}")
    print(f"  Profiles saved to: {os.path.abspath(output_path)}")
    print(f"  Thumbnails saved to: {os.path.abspath(thumb_dir)}")


def do_scrape_comments(index, video_dir):
    """Scrape comments for every non-unavailable video."""
    comments_dir = os.path.join(video_dir, "comments")
    os.makedirs(comments_dir, exist_ok=True)

    videos = [(vid, e) for vid, e in index.data.items()
              if e.get("status") != "unavailable" and vid not in index.excluded_ids]
    total = len(videos)

    if not videos:
        print("  No videos to scrape comments for.")
        return

    print(f"  Scraping comments for {total} video(s)...")
    print(f"  Already-scraped videos will be skipped.")
    print()

    done = skipped = failed = 0

    for i, (vid, e) in enumerate(videos, 1):
        pct = i / total * 100
        pb = bar(pct, 26)
        print(f"\r  {pb}  {i}/{total}  done={done} skip={skipped} fail={failed}  ",
              end="", flush=True)

        comment_file = os.path.join(comments_dir, f"{vid}.json")
        if os.path.exists(comment_file):
            skipped += 1
            continue

        url = canonical_yt_url(vid)
        try:
            r = subprocess.run(
                ["yt-dlp", "--dump-single-json", "--write-comments",
                 "--no-warnings", "--socket-timeout", "30", url],
                capture_output=True, text=True, timeout=120,
            )
            if r.returncode == 0 and r.stdout.strip():
                raw = next(
                    (l for l in reversed(r.stdout.splitlines()) if l.strip().startswith("{")),
                    None,
                )
                if raw:
                    d = json.loads(raw)
                    comments = d.get("comments") or []
                    with open(comment_file, "w", encoding="utf-8") as f:
                        json.dump(comments, f, indent=2, ensure_ascii=False)
                    done += 1
                else:
                    failed += 1
            else:
                failed += 1
        except Exception:
            failed += 1

        time.sleep(0.3)

    clear_line()
    print(f"  Done. Scraped: {done}  Skipped (already done): {skipped}  Failed: {failed}")
    print(f"  Comments saved to: {os.path.abspath(comments_dir)}")


# ── Menu helpers ──────────────────────────────────────────────────────────────

def ask(prompt, choices):
    while True:
        ans = input(prompt).strip().lower()
        if ans in choices:
            return ans
        print(f"  Please enter one of: {' / '.join(choices)}")


def print_header():
    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║   YTP Backup — YouTube Index & Downloader        ║")
    print("╚══════════════════════════════════════════════════╝")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import io
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--site-dir",        default=DEFAULT_SITE_DIR)
    p.add_argument("--video-dir",       default=DEFAULT_VIDEO_DIR)
    p.add_argument("--docs-dir",        default=DEFAULT_DOCS_DIR)
    p.add_argument("--format",          default=DEFAULT_FORMAT)
    p.add_argument("--rate-limit",      default=None)
    p.add_argument("--retry-failed",    action="store_true")
    p.add_argument("--stats",           action="store_true",
                   help="Write stats.md and exit")
    p.add_argument("--chronology",      action="store_true",
                   help="Print top-20 most-viewed videos by year and exit")
    p.add_argument("--dump-poopers",    metavar="OUTPUT", nargs="?", const="poopers.md",
                   help="Dump pooper table to Markdown file (default: poopers.md)")
    p.add_argument("--find-mirrors",    action="store_true",
                   help="Search YouTube for reuploads of unavailable videos")
    p.add_argument("--scrape-comments", action="store_true",
                   help="Scrape comments for all indexed videos")
    p.add_argument("--scrape-profiles", action="store_true",
                   help="Scrape channel profiles and save to docs/ytpoopers.json")
    p.add_argument("--year-limit",      type=int, default=2016,
                   help="Limit downloads to videos published until this year (for language mode)")
    args, _ = p.parse_known_args()

    if not os.path.isdir(args.site_dir):
        print(f"[!] site_dir not found: {args.site_dir}")
        sys.exit(1)

    if args.stats or args.chronology or args.dump_poopers or args.find_mirrors or args.scrape_comments or args.scrape_profiles:
        index = VideoIndex(args.video_dir, args.docs_dir)
        index.load()
        if args.stats:
            do_stats(index)
        if args.chronology:
            do_chronology(index)
        if args.find_mirrors:
            do_find_mirrors(index)
        if args.scrape_comments:
            do_scrape_comments(index, args.docs_dir)
        if args.scrape_profiles:
            do_scrape_profiles(index, args.docs_dir)
        return

    print_header()
    print(f"  Site dir:  {os.path.abspath(args.site_dir)}")
    print(f"  Video dir: {os.path.abspath(args.video_dir)}")
    print(f"  Docs dir:  {os.path.abspath(args.docs_dir)}")
    print(f"  Sections:  {', '.join(SCAN_SECTIONS)}")
    print()
    print("  What do you want to do?")
    print()
    print("  1  Fetch missing metadata")
    print("       Fetch missing title / description / channel URL /")
    print("       tags from YouTube for all videos in index.")
    print()
    print("  2  Download indexed videos")
    print("       Download all pending videos in the index.")
    print()
    print("  3  Scrape channels")
    print("       Scrape all index channels + allowed-channel list;")
    print("       add YTP matches to 'Youtube' section.")
    print()
    print("  4  Download 'Youtube' section")
    print("       Download only videos scraped via mode 3.")
    print()
    print("  5  Section Download (Filter by SCAN_SECTIONS)")
    print()
    print("  6  Download YTPs from selected language")
    print("       Download videos from ALLOWED_CHANNELS, 'YTP nostrane', or 'YTP fai da te'.")
    print()
    print("  7  Stats  →  stats.md")
    print("       Section & channel breakdown (SCAN_SECTIONS only).")
    print()
    print("  8  Chronology")
    print("       Top 20 most-viewed videos, sorted by year.")
    print()
    print()
    print("  9 Find mirror videos")
    print("       Search YouTube for reuploads of unavailable videos.")
    print()
    print("  10 Scrape comments")
    print("       Fetch comments for every indexed video.")
    print()
    print("  11/12 Scrape channel profiles")
    print("       Scrape name, description, thumbnail, subscribers,")
    print("       creation date for every channel → docs/ytpoopers.json")
    print()
    print("  13 Download 'Risorse' & 'Old sources'")
    print("       Download only videos from these sections into a single folder.")
    print()
    print("  q  Quit")
    print()
    choice = ask("  Choice [1-13/q]: ",
                 {"1","2","3","4","5","6","7","8","9","10","11","12","13","q"})

    if choice == "q":
        sys.exit(0)

    print()

    index = VideoIndex(args.video_dir, args.docs_dir)
    index.load()

    if choice in ("1"):
        do_update_index(index)
        print()
    if choice in ("2"):
        do_download(index, args.video_dir, args.format, args.rate_limit, args.retry_failed)
    if choice == "3":
        do_scrape_channels(index)
        print()
    if choice == "4":
        do_download_youtube(index, args.video_dir, args.format, args.rate_limit, args.retry_failed)
    if choice == "5":
        do_download_by_section(index, args.video_dir, args.format, args.rate_limit)
    if choice == "6":
        print("\nSelect Language:")
        print("1. Italian")
        print("2. English")
        print("3. German")
        print("4. French")
        print("5. Russian")
        lang_choice = input("Language Choice: ").strip()
        
        selected_list = []
        if lang_choice == "1": selected_list = ITALIAN_CHANNELS
        elif lang_choice == "2": selected_list = ENGLISH_CHANELS
        elif lang_choice == "3": selected_list = GERMAN_CHANNELS
        elif lang_choice == "4": selected_list = FRENCH_CHANNELS
        elif lang_choice == "5": selected_list = RUSSIAN_CHANNELS
        
        if selected_list:
            do_download_language(index, args.video_dir, args.format, args.rate_limit, args.retry_failed, selected_list, year_limit=args.year_limit)
        else:
            print("Invalid language selection or empty list.")
    if choice == "7":
        do_stats(index)

    if choice == "8":
        do_chronology(index)

    if choice == "9":
        do_find_mirrors(index)

    if choice == "10":
        do_scrape_comments(index, args.docs_dir)

    if choice == "11":
        do_scrape_profiles(index, args.docs_dir)

    if choice == "12":
        do_scrape_profiles(index, args.docs_dir)

    if choice == "13":
        do_download_risorse(index, args.video_dir, args.format, args.rate_limit, args.retry_failed)

    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        print("\n[!] Unexpected error:")
        traceback.print_exc()
    except KeyboardInterrupt:
        print("\n  Interrupted.")
    finally:
        print()
        try:
            if sys.stdin.isatty():
                input("  Press Enter to close...")
        except (EOFError, OSError):
            pass
