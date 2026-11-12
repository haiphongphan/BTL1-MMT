\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{hcmut-report}

\DeclareOption*{\PassOptionsToClass{\CurrentOption}{article}}
\ProcessOptions\relax
\LoadClass[a4paper]{article}

% Biên dịch bằng pdfLaTeX (TeX Live): vntex + font T5 cho tiếng Việt
\RequirePackage[utf8]{vntex,inputenc}
\RequirePackage{anyfontsize,amsmath,amssymb}
\RequirePackage[T5]{fontenc}

\RequirePackage{geometry,graphicx,indentfirst,setspace}
\RequirePackage{booktabs}
\RequirePackage[center]{caption}

\RequirePackage{fancyhdr,lastpage,hyperref}
\RequirePackage[super,square,sort]{natbib}

% Change to '[english,vietnamese]' to use Vietnamese labels
\RequirePackage[english,vietnamese]{babel}

% Custom commands
\newcommand{\Month}{
  \ifcase \the\month
  \or January
  \or February
  \or March
  \or April
  \or May
  \or June
  \or July
  \or August
  \or September
  \or October
  \or November
  \or December
  \fi
}

\newcommand{\@upperuniname}{Vietnam National University Ho Chi Minh City}
\newcommand{\@uniname}{Ho Chi Minh City University of Technology}

\newcommand{\@deptname}{Faculty of Computer Science and Engineering}
\newcommand{\deptname}[1]{\renewcommand{\@deptname}{#1}}

\newcommand{\@coursename}{Course name}
\newcommand{\coursename}[1]{\renewcommand{\@coursename}{#1}}

\newcommand{\@reporttype}{Assignment Report}
\newcommand{\reporttype}[1]{\renewcommand{\@reporttype}{#1}}

\renewcommand{\@title}{Report title}

\newcommand{\@advisor}{Advisor}
\newcommand{\advisor}[1]{\renewcommand{\@advisor}{#1}}

\newcommand{\@stuname}{STUDENT:\ H (xxxxxxx)}
\newcommand{\stuname}[1]{\renewcommand{\@stuname}{#1}}

\newcommand{\@reportplace}{Ho Chi Minh City}
\newcommand{\reportplace}[1]{\renewcommand{\@reportplace}{#1}}

\newcommand{\@reporttime}{\Month \the\year}
\newcommand{\reporttime}[1]{\renewcommand{\@reporttime}{#1}}

% Configurations (bố cục theo template của Khoi.zip)
%\graphicspath{{subdir1/}{subdir2/}{subdir3/}...{subdirn/}}

\geometry{
  paper=a4paper,
  vmargin=3cm,
  hmargin=2cm,
  includeheadfoot=true,
  headheight=30pt
}

\onehalfspacing

% Set depth of numbering for sections and table of contents
\setcounter{secnumdepth}{3}
\setcounter{tocdepth}{3}

% Set depth of numbering for counters
\AtBeginDocument{\counterwithin{equation}{section}}
\AtBeginDocument{\counterwithin{table}{section}}
\AtBeginDocument{\counterwithin{figure}{section}}

\AtBeginDocument{\renewcommand*\bibname{References}}

\hypersetup{
  colorlinks=true,
  linkcolor=black,
  citecolor=red,
  urlcolor=blue
}

\pagestyle{fancy}

% Header: logo + tên trường/khoa bên trái
\renewcommand{\headrulewidth}{0.3pt}
\fancyhead{}
\fancyhead[L]{
  \begin{tabular}{rl}
    \begin{picture}(25,15)(0,0)
      \put(0,-8){\includegraphics[width=8mm,height=8mm]{graphics/hcmut.png}}
    \end{picture}
    \begin{tabular}{l}
      \textbf{\ttfamily University of Technology, Ho Chi Minh City} \\
      \textbf{\ttfamily \@deptname}
    \end{tabular}
  \end{tabular}
}

% Footer: loại báo cáo bên trái, "Page x/y" bên phải
\renewcommand{\footrulewidth}{0.3pt}
\fancyfoot{}
\fancyfoot[L]{\scriptsize \ttfamily \@reporttype}
\fancyfoot[R]{\scriptsize \ttfamily Page {\thepage}/\pageref*{LastPage}}

% Custom pages and sections
\newcommand{\coverpage}{
  \thispagestyle{empty}

  \vspace*{-3\baselineskip}
  \begin{center}
    \MakeUppercase{\@upperuniname}

    \MakeUppercase{\@uniname}

    \MakeUppercase{\@deptname}
    \vfill
    \includegraphics[scale=0.4]{graphics/hcmut.png}
    \vfill
    \begin{tabular}{c}
      \textbf{\Large \@coursename}                                 \\
      {}                                                           \\
      \midrule                                                     \\
      {}                                                           \\
      \textbf{\Large \@reporttype}                                 \\
      {}                                                           \\
      \textbf{\LARGE \parbox[c]{0.85\linewidth}{\centering \@title}} \\
      {}                                                           \\
      \bottomrule
    \end{tabular}
    \vfill%
    \begin{tabular}{rll}
      Giảng viên hướng dẫn: \@advisor \\
      Sinh viên thực hiện: \@stuname
    \end{tabular}
    \vfill
    \MakeUppercase{\@reportplace, \@reporttime}
  \end{center}

  \clearpage
}
