open Printf

(* オプション *)
let is_hour = ref false (* 時間ごとのアクセス件数を表示するかどうか *)
let is_host = ref false (* リモートホスト別のアクセス件数を表示するかどうか *)
let is_period = ref false (* 期間を指定するか *)

(* 指定した期間 *)
let ref_start = ref "00/Jan/0000"
let ref_end = ref "00/Jan/0000"

(* ログファイル名 *)
let log_file_names = ref []

(* ログデータを保管 *)
let log_data = ref []

(* 集計データ *)
let hour_data : ((string * int ref) list) ref = ref []
let host_data : ((string * int ref) list) ref = ref []

(* OCaml 4.04.0から追加されたsplit_on_char関数を利用 *)
(* Strモジュールは環境によっては使えない可能性があるため *)
let split s c = String.split_on_char c s

let set_start s = 
  ref_start := s; is_period := true
let set_end s = ref_end := s

(* ログファイル名の記録 *)
let add_file_name s = 
  log_file_names := s :: !log_file_names

(* ソート *)
let sort_by_tuple f order x y = order (compare (f x) (f y))

(* タプルの1つ目の要素で昇順にソート *)
let sort_by_fst_ascend x y = sort_by_tuple fst (fun a -> a) x y 

(* タプルの2つ目の要素で降順にソート *)
let sort_by_snd_descend x y = sort_by_tuple snd (fun a -> a * -1) x y

(* 月の英語表示を数字に変換 *)
let month_to_int m =
  match m with
  | "Jan" -> 1  | "Feb" -> 2  | "Mar" -> 3 
  | "Apr" -> 4  | "May" -> 5  | "Jun" -> 6
  | "Jul" -> 7  | "Aug" -> 8  | "Sep" -> 9
  | "Oct" -> 10 | "Nov" -> 11 | "Dec" -> 12
  | _ -> raise (Invalid_argument m) (* 月を表す文字列でないときは例外 *)

(* 引数(日付)が指定した期間に含まれているか *)
let check_period date = (* date: 01/Jan/2000 等の形 *)
  let map f l = List.rev (List.rev_map f l) in (* 末尾再帰なmap *)
  let date_list = map (fun x -> split x '/') [!ref_start; date; !ref_end] in (* 年月日に分解 *)
  let third x = List.nth x 2 in
  let second x = List.nth x 1 in
  let first x = List.nth x 0 in
  let between l = (* 3要素のリストをとってそれが順序通りになっているか *)
    compare (third l) (second l) >= 0 && compare (second l) (first l) >= 0 in
  if not (between (map third date_list)) (* 年で比較 *)
  then false
  else
    let num_date = (* 月の部分の文字列チェック *)
      try map month_to_int (map second date_list) with Invalid_argument m -> raise (Invalid_argument m) in
    if not (between num_date) (* 月で比較 *)
    then false
    else if not (between (map first date_list)) (* 日で比較 *)
    then false
    else true

(* 集計を行う関数 *)
(* 一度にホストと時間の両方を集計してhour_data, host_dataに保存 *)
let aggregate () = 
  let rec aggregate_sub l =
    match l with
    | [] -> ()
    | x :: xs ->
       let req_time = List.hd (List.tl (split (List.hd (split x ']')) '[')) in (* %tの部分の取得 *)
       let is_between_period = (* 指定した期間に含まれているか判定 *)
         if !is_period 
         then 
           let date = List.hd (split req_time ':') in
           try check_period date with Invalid_argument _ -> prerr_endline "syntax error of date"; exit 0
         else true 
       in
       (* 期間に含まれているかどうかで分岐 *)
       if not is_between_period 
       then aggregate_sub xs (* 期間に含まれてないときのログは飛ばす *)
       else 
         let hour = List.nth (split req_time ':') 1 in (* logから時間を取得 *)

         let host = List.hd (split x ' ') in (* logからホスト名を取得 *)

         (* 集計 *)
         if List.mem_assoc hour !hour_data
         then (* 既に時間 hour のキーがあったとき *)
           begin
             incr (List.assoc hour !hour_data);
             if List.mem_assoc host !host_data
             then (* 既にホスト host のキーがあったとき *)
               incr (List.assoc host !host_data)
             else (* ホスト host のキーがないとき *)
               host_data := (host, ref 1) :: !host_data
           end
         else (* 時間 hour の記録がなかったとき *)
           if List.mem_assoc host !host_data
           then (* 既にホスト host のキーがあったとき *)
             begin
               incr (List.assoc host !host_data); 
               hour_data := (hour, ref 1) :: !hour_data
             end
           else (* ホスト host のキーがないとき *)
             begin (*   ((string * int ref) list) ref   *)
               host_data := (host, ref 1) :: !host_data;
               hour_data := (hour, ref 1) :: !hour_data
             end;
         aggregate_sub xs
  in
  aggregate_sub !log_data

(* 集計結果を表示 *)
let print_result fst_msg data =
  print_endline fst_msg;
  let rec print_sub l =
    match l with
    | [] -> ()
    | x :: xs -> 
       print_string ((fst x) ^ ": "); print_int !(snd x); print_endline ""; print_sub xs
  in print_sub data

(* 引数の入力チャネルをn_readバイトずつ読み込んでlog_dataに保存 *)
(* log_data: 1行を1要素としたリスト *)
let read_log ic n_read = 
  let rec read_log_sub n = (* nバイト(正確にはnバイトを超えた時の行まで)読み込むかEOFまで繰り返す *)
    let line = input_line ic in 
    let n_byte = Bytes.length (Bytes.of_string line) in
    log_data := line :: !log_data; (* ログデータを保存 *)
    if (n - n_byte) > 0
    then read_log_sub (n - n_byte)
    else ()
  in
  try read_log_sub n_read
  with End_of_file -> raise End_of_file (* EOFまで読み込む *)

(* ログファイルを読み込み *)
let rec read log =
  match log with
  | [] -> ()
  | x :: xs ->
     let ic = open_in x in
     let n_read = 1073741824 in (* 1度に読み込むバイト数. 今回は1GBとした *)
     let aggregate_clear () = aggregate (); log_data := [] in (* log_dataを集計して空にする *)
     let rec read_sub n = 
       if n = 0 then read xs
       else
         try read_log ic n_read; aggregate_clear (); read_sub (n-1) (* 同じファイルを分割して読み込み *)
         with End_of_file -> close_in ic; aggregate_clear (); read xs (* 別のファイルを読み込み *)
     in 
     (* ファイルの最大分割数を計算して再帰 *)
     read_sub ((in_channel_length ic - pos_in ic) / n_read + 1) 
                 

(* オプション引数の設定 *)    
let speclist = [
  ("-hour", Arg.Set is_hour, " Print the number of accesses of each time zone");
  ("-host", Arg.Set is_host, " Print list of accesses");
  ("-period", Arg.Tuple [Arg.String set_start; Arg.String set_end], " Set start period and end period");
  ("-file", Arg.Rest add_file_name, " For multiple log files");
]

let () = 
  let usagemsg = "This is a usage message." in
  Arg.parse speclist (fun _ -> prerr_endline "invaild option") usagemsg; (* コマンドライン引数のparse *)
  read !log_file_names; (* ログファイル読み込み *)
  let hour_sorted = List.sort sort_by_fst_ascend !hour_data in
  let host_sorted = List.sort sort_by_snd_descend !host_data in
  if List.length !log_file_names = 0 then begin prerr_endline "no log files ..."; exit 0 end (* ログファイルが無いときのエラー出力 *)
  else (* 結果の表示 *)
    begin
      print_endline ("Aggregated log data" ^ (if !is_period then " from " ^ !ref_start ^ " to " ^ !ref_end else ""));
       print_endline "\n==================";
      (if !is_hour then print_result "Number of accesses of each hour\nhour: accesses\n------------------" hour_sorted);
       print_endline "\n==================";
      (if !is_host then print_result "Number of accesses of each host\nhost: accesses\n------------------" host_sorted)
    end
